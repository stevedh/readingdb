
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

struct config {
  int commit_interval;          /* seconds */
  loglevel_t loglevel;
  char data_dir[FILENAME_MAX];
  unsigned short port;
  int cache_size;
};
struct config conf;

struct itimerval global_itimer;

#define min(X,Y) ((X) < (Y) ? (X) : (Y))
#define MAXRECS 10000 // (((MAXQUERYSET) < (IPC_MAX_RECORDS)) ? (MAXQUERYSET) : (IPC_MAX_RECORDS))
int bucket_sizes[NBUCKETSIZES] = {60 * 5,  /* five minutes */
                                  60 * 60, /* one hour */
                                  60 * 60 * 24}; /* one day */

sig_atomic_t do_shutdown = 0;
void sig_shutdown(int arg) {
  do_shutdown = 1;
}
void signal_setup() {
  sigset_t old, set;
  signal(SIGINT, sig_shutdown);

  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  sigprocmask(SIG_BLOCK, &set, &old);
}

FREELIST(struct ipc_command, dirty_data);

/* open databases and the enviroment */
struct {
  DB *dbp;
  char dbfile[128];

  /* dirty data */
  struct hashtable *dirty_data;
  pthread_mutex_t lock;
} dbs[MAX_SUBSTREAMS];
DB_ENV *env;

/* shutdown locks */
pthread_mutex_t shutdown_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t shutdown_cond = PTHREAD_COND_INITIALIZER;

/* concurrency limit and wait for workers to exit */
pthread_mutex_t worker_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t worker_cond = PTHREAD_COND_INITIALIZER;
int worker_count = 0;
sem_t worker_count_sem;

#define WORKER_ADD     {   sem_wait(&worker_count_sem);        \
pthread_mutex_lock(&worker_lock);                              \
    worker_count ++;                                           \
    pthread_mutex_unlock(&worker_lock); }

#define WORKER_REMOVE  { pthread_mutex_lock(&worker_lock);     \
    worker_count --;                                           \
    pthread_cond_broadcast(&worker_cond);                      \
    pthread_mutex_unlock(&worker_lock);                        \
    sem_post(&worker_count_sem); }

#define WORKER_WAIT { pthread_mutex_lock(&worker_lock);        \
    info("waiting for %i clients to finish\n", worker_count);  \
    while (worker_count > 0)                                   \
      pthread_cond_wait(&worker_cond, &worker_lock);           \
    pthread_mutex_unlock(&worker_lock); }

pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;
struct stats stats = {0, 0, 0};
#define INCR_STAT(STAT) { pthread_mutex_lock(&stats_lock);     \
    stats.STAT ++;                                             \
    pthread_mutex_unlock(&stats_lock); }

unsigned int hash_streamid(void *streamid) {
  unsigned long long *v = streamid;
  return *v;
}

int eqfn_streamid(void *k1, void *k2) {
  unsigned long long *v1 = k1, *v2 = k2;
  return *v1 == *v2;
}

void default_config(struct config *c) {
  c->commit_interval = 1;
  c->loglevel = LOGLVL_INFO;
  strcpy(c->data_dir, DATA_DIR);
  c->port = 4242;
  c->cache_size = 32;
}

void db_open(struct config *conf) {
  int i;
  int ret;
  int cache_size;
  int oflags;

  if ((ret = db_env_create(&env, 0)) != 0) {
    fatal("ENV CREATE: %s\n", db_strerror(ret));
    exit (1);
  }

  /* with such large keys we need more cache to avoid constant disk
     seeks. */
  cache_size = conf->cache_size * 1e6;
  if ((ret = env->set_cachesize(env, 0, cache_size, 0)) != 0) {
    fatal("Error allocating cache error: %s\n", db_strerror(ret));
    exit(1);
  }

  oflags =  DB_INIT_MPOOL | DB_CREATE | DB_THREAD | DB_INIT_LOCK |
    DB_INIT_LOG |  DB_INIT_TXN | DB_RECOVER;

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

/*     if ((ret = dbs[i].dbp->set_bt_compress(dbs[i].dbp, bdb_compress, bdb_decompress)) != 0) { */
/*       fatal("set_bt_compress: %s\n", db_strerror(ret)); */
/*       exit(1); */
/*     } */

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
    warn("CONFUSED\n");
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
  info("Splitting bucket stream_id: %i anchor: 0x%x len: %i elts: %i\n", 
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
  info("Spliting up %i records\n", v->n_valid);
  for (i = 0; i <= v->n_valid; i++) {
    if (i < v->n_valid &&
        v->data[i].timestamp < new_k.timestamp + new_v->period_length) {
    } else {
      /* commit the old data under the new key */
      new_v->n_valid = i - data_start_idx;
      new_v->tail_timestamp = v->data[i-1].timestamp;

      info("writing new bucket anchor 0x%x size %i nvalid %i idx: %i\n", 
           new_k.timestamp, new_v->period_length, new_v->n_valid, data_start_idx);
      debug(" extra info [%i] %i %i\n", i, new_v->tail_timestamp, v->data[i-1].timestamp);
      printf("%i %i\n", new_v->n_valid, i);
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

int add(DB *dbp, ReadingSet *rs) {
  int cur_rec, ret;
  DBC *cursorp;
  struct rec_key cur_k;
  struct rec_val cur_v;
  DB_TXN *tid = NULL;
  unsigned char buf[POINT_OFF(MAXBUCKETRECS + NBUCKETSIZES)];
  struct rec_val *v = (struct rec_val *)buf;
  struct point *rec_data = v->data;
  bool_t bucket_dirty = FALSE, bucket_valid = FALSE;

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
      assert(POINT_OFF(v->n_valid) < sizeof(buf));
      if (bucket_valid == TRUE && 
          (ret = put(dbp, tid, &cur_k, v, POINT_OFF(v->n_valid))) < 0) {
        warn("error writing back data: %s\n", db_strerror(ret));
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
      info("Splitting buckets since this one is full!\n");
      /* start by writing the current bucket back */
      assert(POINT_OFF(v->n_valid) < sizeof(buf));
      if (bucket_valid == TRUE && 
          (ret = put(dbp, tid, &cur_k, v, POINT_OFF(v->n_valid))) < 0) {
        bucket_valid = FALSE;
        warn("error writing back data: %s\n", db_strerror(ret));
      }

      if (split_bucket(dbp, cursorp, tid, &cur_k) < 0)
        goto abort;
      bzero(&cur_k, sizeof(cur_k));
      bzero(&cur_v, sizeof(cur_v));
    }
  }

  if (bucket_valid && bucket_dirty) {
    debug("flushing bucket back to db\n");
    assert(POINT_OFF(v->n_valid) < sizeof(buf));
    if ((ret = put(dbp, tid, &cur_k, v, POINT_OFF(v->n_valid))) < 0) {
      warn("error writing back data: %s\n", db_strerror(ret));
    }
  }

  cursorp->close(cursorp);

  if ((ret = tid->commit(tid, 0)) != 0) {
    fatal("transaction commit failed: %s\n", db_strerror(ret));
    do_shutdown = 1;
  }
  return 0;

 abort:
  cursorp->close(cursorp);
  warn("Aborting transaction\n");

  if ((ret = tid->abort(tid)) != 0) {
    fatal("Could not abort transaction: %s\n", db_strerror(ret));
    // do_shutdown = 1;
    assert(0);
  }
  return -1;
}

int add_enqueue(ReadingSet *rs, Response *reply) {
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

    if (add(dbs[rs->substream].dbp, rs) < 0) {
      sleep(rand() % 1 );
      warn("Transaction aborted... retrying\n");
      if (add(dbs[rs->substream].dbp, rs) < 0) {
        warn("Retry failed... giving up\n");
        INCR_STAT(failed_adds);
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
        
        if (add(dbs[dbid].dbp, val) < 0) {
          warn("Transaction aborted in commit thread... retrying\n");
          sleep(rand() % 10 );
          if (add(dbs[dbid].dbp, val) < 0) {
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
  pthread_cond_broadcast(&shutdown_cond);
  pthread_mutex_unlock(&shutdown_lock);
}

void query(DB *dbp, Query *q, Response *r) {
  int ret;
  DBC *cursorp;
  struct rec_key k;
  struct rec_val v;
  unsigned long long starttime, endtime;
  int streamid;
  
  streamid = q->streamid;
  starttime = q->starttime;
  endtime = q->endtime;
  debug("starting query id: %i start: %i end: %i\n", streamid, starttime, endtime);

  /* set up the query key */
  k.stream_id = streamid;
  k.timestamp = starttime - (starttime % bucket_sizes[NBUCKETSIZES-1]);

  ret = dbp->cursor(dbp, NULL, &cursorp, 0);
  if (cursorp == NULL) {
    dbp->err(dbp, ret, "cursor");
    return;
  }

  if (get_partial(cursorp, DB_SET_RANGE, &k, &v, sizeof(struct rec_val), 0) < 0) {
    goto done;
  }

  do {
    int i;
    int read_recs = min(v.n_valid, MAXRECS - r->data->n_data);
    struct point bucket[MAXBUCKETRECS + NBUCKETSIZES];
    debug("examining record start: 0x%x length: %i streamid: %i\n", 
          k.timestamp, v.period_length, k.stream_id);
    if (streamid != k.stream_id) break;
    if (k.timestamp >= endtime) break;
    if (r->data->n_data >= MAXRECS) break;

    if (get_partial(cursorp, DB_SET, &k, bucket,
                    sizeof(struct point) * read_recs,
		    sizeof(struct rec_val)) < 0) {
     goto next;
    }
    for (i = 0; i < read_recs; i++) {
      if (bucket[i].timestamp >= starttime &&
          bucket[i].timestamp < endtime) {
        _rpc_copy_reading(r->data->data[r->data->n_data++], &bucket[i]);
      }
    }
    debug("query: added %i/%i records\n", r->data->n_data, v.n_valid);
  next:
    ;
  } while (get_partial(cursorp, DB_NEXT, &k, &v, 
                       sizeof(struct rec_val), 0) == 0);

  debug("returning %i records\n", r->data->n_data);
  r->error = RESPONSE__ERROR_CODE__OK;
  
 done:
  cursorp->close(cursorp);
}

void query_nearest(DB *dbp, Nearest *n, Response *rs) {
  int ret;
  DBC *cursorp;
  struct rec_key k;
  struct rec_val v;
  unsigned long long starttime;
  int streamid, i;
  int direction = n->direction == NEAREST__DIRECTION__NEXT ? DB_NEXT : DB_PREV;
  int ret_n = n->has_n ? min(MAXRECS, n->n) : 1;

  streamid = n->streamid;
  starttime = n->reference;
  debug("starting nearest query id: %i reference: %i\n", streamid, starttime);

  /* set up the query key */
  k.stream_id = streamid;
  if (direction == DB_NEXT) {
    k.timestamp = starttime - (starttime % bucket_sizes[NBUCKETSIZES-1]);
  } else if (direction == DB_PREV) {
    k.timestamp = starttime - (starttime % bucket_sizes[0]);
  }

  rs->error = RESPONSE__ERROR_CODE__OK;
  rs->data->n_data = 0;

  ret = dbp->cursor(dbp, NULL, &cursorp, 0);
  if (cursorp == NULL) {
    dbp->err(dbp, ret, "cursor");
    return;
  }

  if (get_partial(cursorp, DB_SET_RANGE, &k, &v, sizeof(struct rec_val), 0) < 0) {
    goto done;
  }

  do {
    struct point bucket[MAXBUCKETRECS + NBUCKETSIZES];
    debug("examining record start: %i length: %i streamid: %i\n", 
          k.timestamp, v.period_length, k.stream_id);
    if ((direction == DB_NEXT && k.stream_id > streamid) ||
        (direction == DB_PREV && k.stream_id < streamid)) break;
    if ((direction == DB_NEXT && k.timestamp + v.period_length < starttime) ||
        (direction == DB_PREV && k.timestamp > starttime)) goto next;
    if (get_partial(cursorp, DB_SET, &k, bucket,
                    sizeof(struct point) * v.n_valid,
		    sizeof(struct rec_val)) < 0) {
     goto next;
    }

    for (i = (direction == DB_NEXT ? 0 : v.n_valid - 1);
         (direction == DB_NEXT ? i < v.n_valid : i >= 0);
         (direction == DB_NEXT ? i++ : i--)) {
      if (k.stream_id == streamid && 
          ((direction == DB_NEXT && bucket[i].timestamp > starttime) ||
           (direction == DB_PREV && bucket[i].timestamp < starttime))) {
        /* return */
        _rpc_copy_reading(rs->data->data[rs->data->n_data++], &bucket[i]);
        if (rs->data->n_data >= ret_n)
          goto done;
      } 
    }

  next:
    ;
  } while (get_partial(cursorp, direction, &k, &v,
                       sizeof(struct rec_val), 0) == 0);
 done:
  cursorp->close(cursorp);
}

void usage(char *progname) {
  fprintf(stderr, 
          "\n\t%s [options]\n"
          "\t\t-v                 verbose\n"
          "\t\t-h                 help\n"
          "\t\t-d <datadir>       set data directory\n"
          "\t\t-c <interval>      set commit interval\n"
          "\t\t-p <port>          local port to bind to\n"
          "\t\t-s <cache size>    cache size (MB)\n\n",
          progname);
}

int optparse(int argc, char **argv, struct config *c) {
  char o;
  char *endptr;
  while ((o = getopt(argc, argv, "vhd:c:p:s:")) != -1) {
    switch (o) {
    case 'h':
      usage(argv[0]);
      return -1;
      break;
    case 'v':
      c->loglevel = LOGLVL_DEBUG;
      break;
    case 'd':
      strncpy(c->data_dir, optarg, FILENAME_MAX);
      break;
    case 's':
      c->cache_size = strtol(optarg, &endptr, 10);
      if (endptr == optarg) {
        fatal("Invalid cache size\n");
        return -1;
      }
      break;
    case 'c':
      c->commit_interval = strtol(optarg, &endptr, 10);
      if ((errno == ERANGE && 
           (c->commit_interval == LONG_MAX || 
            c->commit_interval == LONG_MIN)) ||
          (errno != 0 && c->commit_interval == 0) ||
          endptr == optarg) {
        fatal("Invalid commit interval\n");
        return -1;
      }
      break;
    case 'p':
      c->port = strtol(optarg, &endptr, 10);
      if (c->port < 1024 || c->port > 0xffff) {
        fatal("Invalid port\n");
        return -1;
      }
      break;
    }
  }

  info("Commit interval is %i\n", c->commit_interval);
  return 0;
}

void process_pbuf(struct sock_request *request) {
  struct pbuf_header h;
  int current_alloc = 0;
  void *buf;
  Query *q = NULL;
  ReadingSet *rs = NULL;
  Nearest *n = NULL;
  Response response = RESPONSE__INIT;

  while (fread(&h, sizeof(h), 1, request->sock_fp) > 0) {
    if (ntohl(h.body_length) > MAX_PBUF_MESSAGE) 
      goto abort;
    if (ntohl(h.body_length) > current_alloc) {
      if (current_alloc > 0) {
	free(buf);
	current_alloc = 0;
      }

      buf = malloc(ntohl(h.body_length));
      if (buf == NULL)
        goto abort;

      current_alloc = ntohl(h.body_length);
    }
    if (fread(buf, ntohl(h.body_length), 1, request->sock_fp) <= 0)
      goto abort;

    switch (ntohl(h.message_type)) {
    case MESSAGE_TYPE__QUERY:
      q = query__unpack(NULL, ntohl(h.body_length), buf);
      if (q == NULL) goto abort;
      if (q->substream < 0 || q->substream >= MAX_SUBSTREAMS) {
        query__free_unpacked(q, NULL);
        
        goto abort;
      } 
      INCR_STAT(queries);
      debug("query streamid: %i substream: %i start: %i end: %i\n",
           q->streamid, q->substream, q->starttime, q->endtime);
      response.error = RESPONSE__ERROR_CODE__OK;
      response.data = _rpc_alloc_rs(MAXRECS);
      if (!response.data) {
        response.error = RESPONSE__ERROR_CODE__FAIL_MEM;
        rpc_send_reply(request, &response);
        query__free_unpacked(q, NULL);
        goto abort;
      }

      response.data->streamid = q->streamid;
      response.data->substream = q->substream;
      response.data->n_data = 0;
      query(dbs[q->substream].dbp, q, &response);
      rpc_send_reply(request, &response);
      query__free_unpacked(q, NULL);
      _rpc_free_rs(response.data);
      break;
    case MESSAGE_TYPE__READINGSET:
      rs = reading_set__unpack(NULL, ntohl(h.body_length), buf);
      if (!rs) goto abort;
      if (rs->substream < 0 || rs->substream >= MAX_SUBSTREAMS) {
        response.error = RESPONSE__ERROR_CODE__FAIL_PARAM;
        rpc_send_reply(request, &response);
        reading_set__free_unpacked(rs, NULL);
        goto q_abort;
      }
      INCR_STAT(adds);
      add_enqueue(rs, &response);
      reading_set__free_unpacked(rs, NULL);
      break;
    q_abort:
      reading_set__free_unpacked(rs, NULL);
      goto abort;
    case MESSAGE_TYPE__NEAREST:
      debug("Processing nearest command\n");
      n = nearest__unpack(NULL, ntohl(h.body_length), buf);
      if (!n) goto abort;
      if (n->substream < 0 || n->substream >= MAX_SUBSTREAMS) {
        response.error = RESPONSE__ERROR_CODE__FAIL_PARAM;
        rpc_send_reply(request, &response);
        nearest__free_unpacked(n, NULL);
        goto abort;
      }
      response.error = RESPONSE__ERROR_CODE__FAIL_PARAM;
      response.data = _rpc_alloc_rs(n->has_n ? min(n->n, MAXRECS) : 1);
      if (!response.data) {
        response.error = RESPONSE__ERROR_CODE__FAIL_MEM;
        rpc_send_reply(request, &response);
        nearest__free_unpacked(n, NULL);
        goto abort;
      }
      query_nearest(dbs[n->substream].dbp, n, &response);
      rpc_send_reply(request, &response);
      nearest__free_unpacked(n, NULL);
      _rpc_free_rs(response.data);
      INCR_STAT(nearest);
      break;
    }
  }
 abort:
  if (current_alloc > 0)
    free(buf);
}

void *process_request(void *request) {
  struct sock_request *req = (struct sock_request *)request;
  struct timeval timeout;
  FILE *fp = NULL;
#if 0
  char buf[4096];
  int dbid = 0;
  ReadingSet *rs = NULL;
  Response resp = RESPONSE__INIT;
#endif
  
  timeout.tv_sec = 60;
  timeout.tv_usec = 0;  
  if (setsockopt(req->sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0)
    goto done;

  fp = fdopen(req->sock, "r+");
  if (!fp)
    goto done;
  else
    req->sock_fp = fp;

  process_pbuf(req);

#if 0
  while (fgets(buf, sizeof(buf), fp) != NULL) {
    if (memcmp(buf, "echo", 4) == 0) {
      fwrite(buf, strlen(buf), 1, fp);
    } else if (memcmp(buf, "help", 4) == 0) {
      char *msg = "echo <msg>\n"
        "help\n"
        "dbid <streamid>\n"
        "put <id> <timestamp> <seqno> <value> [min] [max]\n"
        "get <id> <start> <end>\n";
      fwrite(msg, strlen(msg), 1, fp);
    } else if (memcmp(buf, "quit", 4) == 0) {
      break;
    } else if (memcmp(buf, "dbid", 4) == 0) {
      if (sscanf(buf, "dbid %i", &dbid) != 1 ||
          dbid < 0 ||
          dbid >= MAX_SUBSTREAMS) {
        char *msg = "-1 Invalid dbid\n";
        fwrite(msg, strlen(msg), 1, fp);

        goto done;
      }
      if (rs != NULL && rs->data->n_data > 0) {
        add_enqueue(rs, NULL);
        rs->data->n_data = 0;
      }
    } else if (memcmp(buf, "put", 3) == 0) {
      int streamid, timestamp, sequence, converted, idx;
      double value, min = LLONG_MIN, max = LLONG_MAX;
      if (rs == NULL)
        rs = _rpc_alloc_rs(SMALL_POINTS);
      if (!rs)
        goto done;

      reading__init(rs->data->data[rs->data->n_data]);
      converted = sscanf(buf, "put %i %i %i %lf %lf %lf", 
                         streamid, timestamp, sequence, 
                         value, min, max);
      if (converted < 3) {
        char *msg = "-2 invalid argument\n";
        fwrite(msg, strlen(msg), 1, fp);
        goto done;
      } 

      if (rs->substream != dbid ||
            substream.streamid != streamid) {
        add_enqueue(rs, NULL);
        cmd.args.add.n = 0;
      }
      cmd->substream = dbid;
      cmd->streamid = streamid;
          
      idx = rs->data->n_data++;
      rs->data->data[rs->data->n_data]->timestamp = timestamp;
      rs->data->data[rs->data->n_data]->seqno = sequence;
      rs->data->data[rs->data->n_data]->value = value;
      rs->data->data[rs->data->n_data]->min = min;
      rs->data->data[rs->data->n_data]->max = max;
      if (sequence != 0)
        rs->data->data[rs->data->n_data]->has_seqno = 1;
      if (converted == 6) {
        rs->data->data[rs->data->n_data]->has_min = 1;
        rs->data->data[rs->data->n_data]->has_max = 1;
      }

      if (rs->data->n_data == SMALL_POINTS) {
        add_enqueue(rs, NULL);
        rs->data->n_data = 0;
      }
      INCR_STAT(adds);

    } else if (memcmp(buf, "get", 3) == 0) {
      int i, len;
      unsigned long long streamid, start, end;
      struct ipc_reply *r = malloc(sizeof(struct ipc_reply) + 
                                   (sizeof(struct point) * 
                                    (MAXRECS + MAXBUCKETRECS) ));
      if (!r) {
        char *msg = "-5 no query buffer\n";
        fwrite(msg, strlen(msg), 1, fp);
        break;
      }

      if (sscanf(buf, "get %llu %llu %llu", &streamid, &start, &end) != 3) {
        char *msg = "-3 invalid get\n";
        fwrite(msg, strlen(msg), 1, fp);
        free(r);
        break;
      }

      /* add any pending data before reusing the buffer */
      if (rs && rs->data->n_data > 0) {
        add_enqueue(rs, NULL);
      }

      cmd.command = COMMAND_QUERY;
      cmd.dbid = dbid;
      cmd.streamid = streamid;
      cmd.args.query.starttime = start;
      cmd.args.query.endtime = end;

      //query(dbs[dbid].dbp, &cmd, r);

      if (r->reply != REPLY_OK) {
        char *msg = "-4 query failed\n";
        fwrite(msg, strlen(msg), 1, fp);
        free(r);
        break;
      }

      len = snprintf(buf, sizeof(buf), "%i\n", r->data.query.nrecs);
      fwrite(buf, len, 1, fp);
      
      for (i = 0; i < r->data.query.nrecs; i++) {
        double bottom = LLONG_MIN + 1, top = LLONG_MAX - 1;
        len = snprintf(buf, sizeof(buf), "%i %i %f",
                       r->data.query.pts[i].timestamp,
                       r->data.query.pts[i].reading_sequence,
                       r->data.query.pts[i].reading);
        if (r->data.query.pts[i].min > bottom ||
            r->data.query.pts[i].max < top) {
          len += snprintf(buf + len, sizeof(buf) - len, " %f %f",
                          r->data.query.pts[i].min,
                          r->data.query.pts[i].max);
        }
        buf[len++] = '\n';
        fwrite(buf, len, 1, fp);
      }

      INCR_STAT(queries);
      free(r);
    } else if (memcmp(buf, "binmode", 7) == 0) {
      process_pbuf(req);
      goto done;
    }
  }
  
  if (cmd.command == COMMAND_ADD &&
      cmd.args.add.n > 0) {
    // add_enqueue(&cmd);
  }
#endif
  
 done:
  debug("closing socket\n");
  INCR_STAT(disconnects);
  if (fp)
    fclose(fp);
  else
    close(req->sock);

  free(request);
  WORKER_REMOVE;
  return NULL;
}

pthread_t * start_threads(struct config *c) {
  pthread_t *thread = malloc(sizeof(pthread_t));
  pthread_attr_t attr;
  size_t stacksize;
  pthread_attr_init(&attr);
  pthread_attr_getstacksize(&attr, &stacksize);
  info("Default pthread stack size: %li\n", stacksize);

  if (c->commit_interval > 0) {
    // pthread_create(thread, NULL, (void *)(void *)commit_data, c);
    // pthread_detach(*thread);
    return thread;
  } else {
    free(thread);
    return NULL;
  }
}

int main(int argc, char **argv) {
  struct timeval last, now, delta;
  int yes;

  sem_init(&worker_count_sem, 0, MAXCONCURRENCY);

  /* so gprof works right */
  getitimer(ITIMER_PROF, &global_itimer);

  log_init();

  default_config(&conf);
  if (optparse(argc, argv, &conf) < 0)
    exit(1);

  stats_init(conf.port);

  log_setlevel(conf.loglevel);

  drop_priv();

  // open the database
  db_open(&conf);
  
  signal_setup();
  gettimeofday(&last, NULL);

  int sock = socket(AF_INET6, SOCK_STREAM, 0);
  struct sockaddr_in6 addr = {
    .sin6_family = AF_INET6,
    .sin6_addr = IN6ADDR_ANY_INIT,
    .sin6_port = htons(conf.port),
  };
  

  if (sock < 0) {
    log_fatal_perror("socket");
    goto close;
  }

  if (bind(sock, (struct sockaddr *)&addr, sizeof(struct sockaddr_in6)) < 0) {
    log_fatal_perror("bind");
    goto close;
  }
  info("listening on port %i\n", conf.port);

  yes = 1;
  if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
    log_fatal_perror("setsockopt: SO_REUSEADDR");
    goto close;
  }

  now.tv_sec = 0;
  now.tv_usec = 1e5;  
  if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &now, sizeof(now)) < 0) {
    log_fatal_perror("setsockopt: SO_RCVTIMEO");
    goto close;
  }

  if (listen(sock, 4096) < 0) {
    log_fatal_perror("listen");
    goto close;
  }

  start_threads(&conf);

  while (!do_shutdown) {
    char addr_buf[256];
    struct sockaddr_in6 remote;
    int client, rc;
    socklen_t addrlen = sizeof(struct sockaddr_in6);
    struct sock_request *req;
    pthread_t thread;
    
    client = accept(sock, (struct sockaddr *)&remote, &addrlen);
    if (client < 0) {
      if (errno != EAGAIN && errno != EINTR) {
        log_fatal_perror("accept");
      }
      goto do_stats;
      continue;
    }
    
    inet_ntop(AF_INET6, &remote.sin6_addr, addr_buf, sizeof(addr_buf));
    debug("Accepted client connection from %s\n", addr_buf);
    INCR_STAT(connects);

    req = malloc(sizeof(struct sock_request));
    if (!req) {
      warn("could not allocate request buffer for client\n");
      close(client);
      continue;
    }

    req->sock = client;

    WORKER_ADD;
    if ((rc = pthread_create(&thread, NULL, process_request, req)) != 0) {
      WORKER_REMOVE;
      close(client);
      free(req);
      warn("could not start new thread for client: [%i] %s\n",
           rc, strerror(rc));
      continue;
    };

    /* this doesn't return any errors we care about */
    pthread_detach(thread);

  do_stats:
    gettimeofday(&now, NULL);
    timeval_subtract(&delta, &now, &last);
    if (delta.tv_sec > 0) {
      int current_workers;
      pthread_mutex_lock(&worker_lock);
      current_workers = worker_count;
      pthread_mutex_unlock(&worker_lock);

      pthread_mutex_lock(&stats_lock);
      float tps = stats.queries + stats.adds;
      tps /= ((float)delta.tv_sec) + (((float)delta.tv_usec) / 1e6);
      info("%li.%06lis: %0.2ftps gets: %i puts: %i put_fails: %i "
           "clients: %i connects: %i disconnects: %i \n",
           delta.tv_sec, delta.tv_usec, tps,
           stats.queries, stats.adds, stats.failed_adds, 
           current_workers, stats.connects, stats.disconnects);
      stats_report(&stats, &now);
      memset(&stats, 0, sizeof(stats));
      pthread_mutex_unlock(&stats_lock);
      gettimeofday(&last, NULL);
    }

  }

  /* don't accept new connections */
  close(sock);

  /* this waits for outstanding client threads to exit */
  WORKER_WAIT;

  /* wait to flush hashtable data and sync */
  info("clients exited, waiting on commit...\n");
  pthread_mutex_lock(&shutdown_lock);
  pthread_cond_broadcast(&shutdown_cond);
  pthread_cond_wait(&shutdown_cond, &shutdown_lock);
  pthread_mutex_unlock(&shutdown_lock);
  info("commit thread exited; closing databases\n");

 close:
  db_close();
  stats_close();
  return 0;
}
