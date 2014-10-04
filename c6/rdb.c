
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <db.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/select.h>
#include <fcntl.h>
#include <semaphore.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <assert.h>
#include <pthread.h>
#include <limits.h>

#include "readingdb.h"
#include "util.h"
#include "logging.h"
#include "hashtable.h"
#include "stats.h"
#include "rpc.h"
#include "pbuf/rdb.pb-c.h"
#include "commands.h"
#include "config.h"
#include "rdb.h"

/* open databases and the enviroment */
struct subdb dbs[MAX_SUBSTREAMS];
DB_ENV *env;

int bucket_sizes[NBUCKETSIZES] = {60 * 5,  /* five minutes */
                                  60 * 60, /* one hour */
                                  60 * 60 * 24}; /* one day */

struct itimerval global_itimer;

/* hash comparisons */
unsigned int hash_streamid(void *streamid) {
  unsigned long long *v = streamid;
  return *v;
}
int eqfn_streamid(void *k1, void *k2) {
  unsigned long long *v1 = k1, *v2 = k2;
  return *v1 == *v2;
}

/* shutdown locks */
pthread_mutex_t shutdown_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t shutdown_cond = PTHREAD_COND_INITIALIZER;

int valid_bucketsize(int length) {
  int i;
  for (i = 0; i < NBUCKETSIZES; i++) {
    if (length == bucket_sizes[i]) return 1;
  }
  return 0;
}

void db_open(struct config *conf) {
  int i;
  int ret;
  unsigned int cache_gb, cache_b;
  int oflags;

  /* so gprof works right */
  getitimer(ITIMER_PROF, &global_itimer);

  if ((ret = db_env_create(&env, 0)) != 0) {
    fatal("ENV CREATE: %s\n", db_strerror(ret));
    exit (1);
  }

  /* with such large keys we need more cache to avoid constant disk
     seeks. */
  cache_gb = conf->cache_size / 1000;
  cache_b = (conf->cache_size % 1000) * 1e6;
  info("allocating %iGB and %iB cache\n", cache_gb, cache_b);
  if ((ret = env->set_cachesize(env, cache_gb, cache_b, 0)) != 0) {
    fatal("Error allocating cache error: %s\n", db_strerror(ret));
    exit(1);
  }

  /* set the number of transactions to the number of threads we might
     have, plus one for the commit thread. default is 20. */
  if ((ret = env->set_tx_max(env, MAXCONCURRENCY+1)) != 0) {
    fatal("set_tx_max: %s\n", db_strerror(ret));
    exit(1);
  }

  oflags =  DB_INIT_MPOOL | DB_CREATE | DB_THREAD | DB_INIT_LOCK |
    DB_INIT_LOG |  DB_INIT_TXN | DB_RECOVER | DB_REGISTER;

  if ((ret = env->open(env, conf->data_dir, oflags, 0)) != 0) {
    fatal("ENV OPEN: %s\n", db_strerror(ret));
    exit(1);
  }
  
  if ((ret = env->set_flags(env, DB_TXN_NOSYNC, 1)) != 0) {
    fatal("set flags: %s\n", db_strerror(ret));
    exit(1);
  }

  for (i = 0; i < MAX_SUBSTREAMS; i++) {
    oflags = DB_CREATE | DB_THREAD | DB_AUTO_COMMIT;

    snprintf(dbs[i].dbfile, sizeof(dbs[i].dbfile), "readings-%i.db", i);
    info("Opening '%s'\n", dbs[i].dbfile);
    
    if ((ret = db_create(&dbs[i].dbp, env, 0)) != 0) {
      fatal("CREATE: %i: %s", i, db_strerror(ret));
      exit(1);
    }

    if ((ret = dbs[i].dbp->set_pagesize(dbs[i].dbp, DEFAULT_PAGESIZE)) != 0) {
      warn("set_pagesize: dbid: %i: %s\n", i, db_strerror(ret));
    }

    if ((ret = dbs[i].dbp->open(dbs[i].dbp,
                                NULL, dbs[i].dbfile, NULL, 
                                DB_BTREE, oflags, 0644)) != 0) {
      fatal("db->open: %s\n", db_strerror(ret));
      exit(1);
    }
    
    pthread_mutex_init(&dbs[i].lock, NULL);
    dbs[i].dirty_data = create_hashtable(100,
                                         hash_streamid,
                                         eqfn_streamid);
  }
}

void db_close() {
  int i, ret;
  for (i = 0; i < MAX_SUBSTREAMS; i++) {
    dbs[i].dbp->close(dbs[i].dbp, 0);
  }

  if ((ret = env->close(env, 0)) != 0) {
    fatal("ENV CLOSE: %s\n", db_strerror(ret));
    exit(1);
  }
}

/* mark a region of time dirty */
void mark_sketch_dirty(struct config *c,
                       unsigned long long streamid, 
                       unsigned long long start, 
                       unsigned long long end) {
  if (c->sketch == 0) {
    return;
  }

  FILE *logfp = fopen(c->sketch_log, "a");
  if (logfp == NULL) {
    warn("unable to open sketch logfile: %s\n", strerror(errno));
  }
  fprintf(logfp, "%llu\t%llu\t%llu\n", streamid, start, end);
  fclose(logfp);
}

int get_bucket(DBC *cursorp, struct rec_key *k, struct rec_val *v) {
  int bucket_size_idx, key_level = -1;
  struct rec_key cur_k;
  for (bucket_size_idx = NBUCKETSIZES-1; bucket_size_idx >= 0; bucket_size_idx--) {
    /* find the start of the current bucket */
    cur_k.stream_id = k->stream_id;
    cur_k.timestamp = k->timestamp - (k->timestamp % bucket_sizes[bucket_size_idx]);
    /* key query */
    /* acquire write locks to avoid deadlock */
    if (get_partial(cursorp, DB_SET | DB_RMW, &cur_k, v, sizeof(struct rec_val), 0) == 0) {
      if (cur_k.timestamp <= k->timestamp &&
          cur_k.timestamp + v->period_length > k->timestamp) {
        *k = cur_k;
        return 1;
      }
    } else if (key_level == -1) {
      key_level = bucket_size_idx;
    }
  }
  if (key_level < 0) {
    warn("CONFUSED streamid: %u timestamp: %u\n",
         k->stream_id, k->timestamp);
    return 1;
  }
  debug("Should be in new bin; size idx %i\n", key_level);
  k->timestamp -= (k->timestamp % bucket_sizes[key_level]);
  return -key_level;
}

int split_bucket(DB *dbp, DBC *cursorp, DB_TXN *tid, struct rec_key *k) {
  int ret, i, new_bucket_idx = -1, data_start_idx;
  char buf[POINT_OFF(MAXBUCKETRECS + NBUCKETSIZES)];
  char new_buf[POINT_OFF(MAXBUCKETRECS + NBUCKETSIZES)];
  struct rec_val *v = (struct rec_val *)buf;
  struct rec_key new_k;
  struct rec_val *new_v = (struct rec_val *)new_buf;
  
  if ((ret = get(cursorp, DB_SET, k, buf, sizeof(buf))) < 0) {
    error("error reading full bucket into memory for split (%s)!\n", db_strerror(ret));
    return -1;
  }
  debug("Splitting bucket stream_id: %i anchor: 0x%x len: %i elts: %i\n", 
       k->stream_id, k->timestamp, v->period_length, v->n_valid);
  if (v->period_length == bucket_sizes[0]) {
    return 0;
  }

  for (i = 0; i < NBUCKETSIZES; i++) {
    if (v->period_length == bucket_sizes[i])
      new_bucket_idx = i - 1;
  }

  new_k.stream_id = k->stream_id;
  new_k.timestamp = k->timestamp;
  new_v->n_valid = 0;
  new_v->period_length = bucket_sizes[new_bucket_idx];
  new_v->tail_timestamp = 0;
  data_start_idx = 0;
  debug("Spliting up %i records\n", v->n_valid);
  for (i = 0; i <= v->n_valid; i++) {
    if (i < v->n_valid &&
        v->data[i].timestamp < new_k.timestamp + new_v->period_length) {
    } else {
      /* commit the old data under the new key */
      new_v->n_valid = i - data_start_idx;
      new_v->tail_timestamp = v->data[i-1].timestamp;

      debug("writing new bucket anchor 0x%x size %i nvalid %i idx: %i\n", 
           new_k.timestamp, new_v->period_length, new_v->n_valid, data_start_idx);
      debug(" extra info [%i] %i %i\n", i, new_v->tail_timestamp, v->data[i-1].timestamp);
      assert(new_v->n_valid >= 0 && new_v->n_valid <= MAXBUCKETRECS + NBUCKETSIZES);

      if (i > data_start_idx) {
        /* copy the data into the new bucket */
        memcpy(new_buf + POINT_OFF(0), buf + POINT_OFF(data_start_idx),
               POINT_OFF(i) - POINT_OFF(data_start_idx));
      }

      assert(POINT_OFF(i - data_start_idx) < sizeof(new_buf));
      if (put(dbp, tid, &new_k, new_v, POINT_OFF(i - data_start_idx)) < 0) {
        warn("put failed\n");
        return -1;
      }

      if (i < v->n_valid) {
        /* start a new key */
        new_k.timestamp = v->data[i].timestamp - 
          (v->data[i].timestamp % bucket_sizes[new_bucket_idx]);
        new_v->n_valid = 0;
        new_v->tail_timestamp = 0;
        data_start_idx = i;
      }
    }
  }
  return 0;
}

int add(struct config *c, DB *dbp, ReadingSet *rs) {
  int cur_rec, ret;
  DBC *cursorp;
  struct rec_key cur_k;
  struct rec_val cur_v;
  DB_TXN *tid = NULL;
  unsigned char buf[POINT_OFF(MAXBUCKETRECS + NBUCKETSIZES)];
  struct rec_val *v = (struct rec_val *)buf;
  struct point *rec_data = v->data;
  bool_t bucket_dirty = FALSE, bucket_valid = FALSE;
  unsigned long long dirty_start = ULLONG_MAX, dirty_end = 0;

  bzero(&cur_k, sizeof(cur_k));
  bzero(&cur_v, sizeof(cur_v));

  if ((ret = env->txn_begin(env, NULL, &tid, 0)) != 0) {
    error("txn_begin: %s\n", db_strerror(ret));
    return -1;
  }
  
  if ((ret = dbp->cursor(dbp, tid, &cursorp, 0)) != 0) {
    error("db cursor: %s\n", db_strerror(ret));
    goto abort;
  }
  if (cursorp == NULL) {
    dbp->err(dbp, ret, "cursor");
    goto abort;
  }

  for (cur_rec = 0; cur_rec < rs->n_data; cur_rec++) {
    debug("Adding reading ts: 0x%x\n", rs->data[cur_rec]->timestamp);
    if (bucket_valid &&
        v->n_valid > 0 &&
        cur_k.stream_id == rs->streamid &&
        cur_k.timestamp <= rs->data[cur_rec]->timestamp &&
        cur_k.timestamp + v->period_length > rs->data[cur_rec]->timestamp) {
      /* we're already in the right bucket; don't need to do anything */
      debug("In valid bucket.  n_valid: %i\n", v->n_valid);
    } else {
      /* need to find the right bucket */
      debug("Looking up bucket\n");
      assert(!bucket_valid || POINT_OFF(v->n_valid) < sizeof(buf));
      if (bucket_valid == TRUE && 
          (ret = put(dbp, tid, &cur_k, v, POINT_OFF(v->n_valid))) < 0) {
        warn("error writing back data: %s\n", db_strerror(ret));
        // we will loose data, aborto the transaction.
        goto abort;
      }
      bucket_valid = FALSE;

      cur_k.stream_id = rs->streamid;
      cur_k.timestamp = rs->data[cur_rec]->timestamp;

      if ((ret = get_bucket(cursorp, &cur_k, &cur_v)) <= 0) {
        /* create a new bucket */

        /* the key has been updated by get_bucket */
        v->n_valid = 0;
        v->period_length = bucket_sizes[-ret];
        v->tail_timestamp = 0;
        debug("Created new bucket anchor: %i length: %i\n", cur_k.timestamp, v->period_length);
      } else {
        debug("Found existing bucket streamid: %i anchor: %i length: %i\n", 
              cur_k.stream_id, cur_k.timestamp, v->period_length);
        if ((ret = get(cursorp, DB_SET | DB_RMW, &cur_k, v, sizeof(buf))) < 0) {
          warn("error reading bucket: %s\n", db_strerror(ret));
          goto abort;
        }
      }
      bucket_valid = TRUE;
    }

    debug("v->: tail_timestamp: %i n_valid: %i\n", v->tail_timestamp, v->n_valid);
    /* start the insert -- we're in the current bucket */
    if (v->tail_timestamp < rs->data[cur_rec]->timestamp ||
        v->n_valid == 0) {
      /* if it's an append or a new bucket we can just write the values */
      /* update the header block */
      v->tail_timestamp = rs->data[cur_rec]->timestamp;
      v->n_valid++;
      /* and the data */
      _rpc_copy_records(&v->data[v->n_valid-1], &rs->data[cur_rec], 1);
      debug("Append detected; inserting at offset: %i\n", POINT_OFF(v->n_valid-1));
    } else {
      struct rec_val *v = (struct rec_val *)buf;
      struct point new_rec;
      int i;
      /* otherwise we have to insert it somewhere. we'll just read out
         all the data and do the insert stupidly. */
      for (i = 0; i < v->n_valid; i++) {
        if (v->data[i].timestamp >= rs->data[cur_rec]->timestamp)
          break;
      }
      debug("Inserting within existing bucket index: %i (%i %i)\n", 
            i, rs->data[cur_rec]->timestamp, v->tail_timestamp);
      /* appends should have been handled without reading back the whole record */
      assert(i < v->n_valid);
      /* we have our insert position */
      if (v->data[i].timestamp == rs->data[cur_rec]->timestamp) {
        /* replace a record */
        debug("Replacing record with timestamp 0x%x\n", rs->data[cur_rec]->timestamp);
        _rpc_copy_records(&v->data[i], &rs->data[cur_rec], 1);
      } else {
        /* shift the existing records back */
        debug("Inserting new record (moving %i recs)\n", v->n_valid - i);
        memmove(&v->data[i+1], &v->data[i], (v->n_valid - i) * sizeof(struct point));
        _rpc_copy_records(&v->data[i], &rs->data[cur_rec], 1);
        v->n_valid++;
        /* and update the header */
      }
    }
    bucket_dirty = TRUE;
    assert(v->n_valid < MAXBUCKETRECS + NBUCKETSIZES);

    if (v->n_valid > MAXBUCKETRECS) {
      debug("Splitting buckets since this one is full!\n");
      /* start by writing the current bucket back */
      assert(POINT_OFF(v->n_valid) < sizeof(buf));
      if (bucket_valid == TRUE && 
          (ret = put(dbp, tid, &cur_k, v, POINT_OFF(v->n_valid))) < 0) {
        bucket_valid = FALSE;
        warn("error writing back data: %s\n", db_strerror(ret));
        goto abort;
      }

      if (split_bucket(dbp, cursorp, tid, &cur_k) < 0)
        goto abort;
      bzero(&cur_k, sizeof(cur_k));
      bzero(&cur_v, sizeof(cur_v));
    }

    /* find the time region this write dirties  */
    if (rs->data[cur_rec]->timestamp < dirty_start) {
      dirty_start = rs->data[cur_rec]->timestamp;
    }
    if (rs->data[cur_rec]->timestamp > dirty_end) {
      dirty_end = rs->data[cur_rec]->timestamp;
    }
  }

  if (bucket_valid && bucket_dirty) {
    debug("flushing bucket back to db\n");
    assert(POINT_OFF(v->n_valid) < sizeof(buf));
    if ((ret = put(dbp, tid, &cur_k, v, POINT_OFF(v->n_valid))) < 0) {
      warn("error writing back data: %s\n", db_strerror(ret));
      goto abort;
    }
  }

  cursorp->close(cursorp);

  if ((ret = tid->commit(tid, 0)) != 0) {
    fatal("transaction commit failed: %s\n", db_strerror(ret));
    // SDH : "If DB_TXN->commit() encounters an error, the transaction
    //  and all child transactions of the transaction are aborted."
    //
    // So, we can just die here.
    // do_shutdown = 1;
    return -1;
  }

  //
  if (dirty_start != ULLONG_MAX && dirty_end != 0) {
    mark_sketch_dirty(c, rs->streamid, dirty_start, dirty_end);
  }

  return 0;

 abort:
  cursorp->close(cursorp);
  warn("Aborting transaction\n");

  if ((ret = tid->abort(tid)) != 0) {
    fatal("Could not abort transaction: %s\n", db_strerror(ret));
    // do_shutdown = 1;
    // SDH : there are no documented error codes for DB_TXN->abort().
    assert(0);
  }
  return -1;
}

int add_enqueue(struct config *c, ReadingSet *rs, Response *reply) {
  unsigned long long key;
  ReadingSet *points;

  debug("add_enqueue\n");

  if (pthread_mutex_lock(&dbs[rs->substream].lock) != 0)
    return -1;

  key = rs->streamid;
  points = hashtable_search(dbs[rs->substream].dirty_data, &key);
  if (points == NULL) {
    unsigned long long *new_key = malloc(sizeof(unsigned long long));
    if (!new_key)
      goto fail;

    points = _rpc_alloc_rs(SMALL_POINTS);
    if (!points) {
      free(new_key);
      goto fail;
    }
    debug("creating new hashtable entry dbid: %i streamid: %i\n", 
          rs->substream, rs->streamid);

    points->streamid = rs->streamid;
    points->substream = rs->substream;
    *new_key = rs->streamid;

    if (!hashtable_insert(dbs[rs->substream].dirty_data, new_key, points)) {
      free(new_key);
      FREELIST_PUT(struct ipc_command, dirty_data, points);
      goto fail;
    }
  }
  if (1 || rs->n_data > SMALL_POINTS - points->n_data) {
    /* do big adds directly */
    debug("writing data directly: streamid: %li n: %i\n",
         key, rs->n_data);
    pthread_mutex_unlock(&dbs[rs->substream].lock);

    if (add(c, dbs[rs->substream].dbp, rs) < 0) {
      sleep(rand() % 1 );
      warn("Transaction aborted... retrying\n");
      if (add(c, dbs[rs->substream].dbp, rs) < 0) {
        warn("Retry failed... giving up\n");
        INCR_STAT(failed_adds);
        return -1;
      }
    }
  } else {
    int i;
    /* there's enough room to defer this add */
    for (i = points->n_data; i < points->n_data + rs->n_data; i++)
      memcpy(points->data[i], rs->data[i - points->n_data], sizeof(Reading));
    points->n_data += rs->n_data;
    debug("Added %i new records for deferred load\n", rs->n_data);
    pthread_mutex_unlock(&dbs[rs->substream].lock);
    return 0;
  }
  return 0;
 fail:
  pthread_mutex_unlock(&dbs[rs->substream].lock);
  return -1;
}

void commit_data(struct config *conf) {
  int dbid = 0, done = 0;
  setitimer(ITIMER_PROF, &global_itimer, NULL);


  pthread_mutex_lock(&shutdown_lock);
  while (!done) {
    struct timespec sleep_time;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    sleep_time.tv_sec = tv.tv_sec + conf->commit_interval;
    sleep_time.tv_nsec = tv.tv_usec * 1e3;

    if (pthread_cond_timedwait(&shutdown_cond, &shutdown_lock, &sleep_time) == 0)
      done = 1;

    debug("commit: checking for new data\n");
    for (dbid = 0; dbid < MAX_SUBSTREAMS; dbid++) {
      unsigned long long *key = NULL;
      ReadingSet *val;

      while (1) {
        if (pthread_mutex_lock(&dbs[dbid].lock) != 0)
          break;
        key = NULL;
        val = hashtable_next(dbs[dbid].dirty_data, (void **)&key);
        if (val == NULL) {
          pthread_mutex_unlock(&dbs[dbid].lock);
          break;
        }
        val = hashtable_remove(dbs[dbid].dirty_data, key);
        pthread_mutex_unlock(&dbs[dbid].lock);
        assert(val != NULL);

        debug("adding dbid: %i streamid: %llu nrecs: %i\n",
              val->substream, val->streamid, val->n_data);
        
        if (add(conf, dbs[dbid].dbp, val) < 0) {
          warn("Transaction aborted in commit thread... retrying\n");
          sleep(rand() % 10 );
          if (add(conf, dbs[dbid].dbp, val) < 0) {
            warn("Transaction retry failed in commit thread... giving up\n");
            INCR_STAT(failed_adds);
          }
        }
        _rpc_free_rs(val);
      }

      debug("Syncing...\n");
      dbs[dbid].dbp->sync(dbs[dbid].dbp, 0);
      
      debug("Done!\n");
      continue;
    }
  }
  pthread_mutex_unlock(&shutdown_lock);
}

/*
 * Periodically write a checkpoint record to the log, and remove the
 * log files.  Since we're not copying them to durable storage for
 * "catastrophic error recovery", we just delete them rathern than
 * allowing them to consume space.  If disabled, you should
 * periodically use db_checkpoint and db_archive to accomplish similar
 * functionality.
 */
void checkpoint_thread(void *p) {
  struct config *c = p;
  struct timespec sleep_time;
  struct timeval tv;
  int done = 0, ret;
  pthread_mutex_lock(&shutdown_lock);
  while (!done) {
    gettimeofday(&tv, NULL);
    sleep_time.tv_sec = tv.tv_sec + c->checkpoint_interval;
    sleep_time.tv_nsec = tv.tv_usec * 1e3;

    if (pthread_cond_timedwait(&shutdown_cond, &shutdown_lock, &sleep_time) == 0)
      done = 1;

    if ((ret = env->txn_checkpoint(env, 10, 0, 0)) != 0) {
      warn("txn_checkpoint: %s\n", db_strerror(ret));
      continue;
    }

    if ((ret = env->log_archive(env, NULL, DB_ARCH_REMOVE)) != 0) {
      warn("log_archive: %s\n", db_strerror(ret));
      continue;
    }

    debug("checkpoint and archive succeeded\n");

  }

  pthread_mutex_unlock(&shutdown_lock);
}

/*
 * Periodically run a deadlock detector to deal with transaction
 * deadlocks, which do occur occasionally.  If this is disabled (by
 * setting the period to zero), you should call db_deadlock
 * occasionally to break deadlocks.
 *
 */
void deadlock_thread(void *p) {
  struct config *c = p;
  struct timespec sleep_time;
  struct timeval tv;
  int done = 0, ret, rejected;
  pthread_mutex_lock(&shutdown_lock);
  while (!done) {
    gettimeofday(&tv, NULL);
    sleep_time.tv_sec = tv.tv_sec + c->deadlock_interval;
    sleep_time.tv_nsec = tv.tv_usec * 1e3;

    if (pthread_cond_timedwait(&shutdown_cond, &shutdown_lock, &sleep_time) == 0)
      done = 1;
    
    if ((ret = env->lock_detect(env, 0, DB_LOCK_DEFAULT, &rejected)) != 0) {
      error("lock_detect: %s\n", db_strerror(ret));
    }

    if (rejected > 0) {
      warn("lock_detect: rejected %i locks\n", rejected);
    } else {
      debug("lock_detect: no locks rejected\n");
    }
  }
  pthread_mutex_unlock(&shutdown_lock);
}

/* start up threads which run continuously
 *  - commit thread
 *  - checkpoint + archive thread
 *  - deadlock thread
 */
pthread_t **start_threads(struct config *c) {
  pthread_t **thread = malloc(sizeof(pthread_t *) * 4);
  pthread_attr_t attr;
  int nthreads = 0;

  if (c->commit_interval > 0) {
    thread[nthreads] = malloc(sizeof(pthread_t));
    pthread_create(thread[nthreads], NULL, (void *)(void *)commit_data, c);
    nthreads ++;
  }

  if (c->deadlock_interval > 0) {
    thread[nthreads] = malloc(sizeof(pthread_t));
    pthread_create(thread[nthreads], NULL, (void *)(void *)deadlock_thread, c);
    nthreads ++;
  } else {
    warn("Not starting deadlock detector!\n");
  }

  if (c->checkpoint_interval > 0) {
    thread[nthreads] = malloc(sizeof(pthread_t));
    pthread_create(thread[nthreads], NULL, (void *)(void *)checkpoint_thread, c);
    nthreads ++;
  } else {
    warn("Not starting checkpoint thread!\n");
  }

  thread[nthreads] = NULL;

  return thread;
}

/* wait on threads we've previously started to stop running */
void stop_threads(pthread_t **threads) {
  int i = 0;
  void *ret;
  pthread_mutex_lock(&shutdown_lock);
  pthread_cond_broadcast(&shutdown_cond);
  pthread_mutex_unlock(&shutdown_lock);

  while (threads[i] != NULL) {
    pthread_join(*threads[i], &ret);
    free(threads[i]);
    i++;
  }
  free(threads);
}

