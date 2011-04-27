
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

struct config {
  int commit_interval;          /* seconds */
  loglevel_t loglevel;
  char data_dir[FILENAME_MAX];
  unsigned short port;
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
struct {
  int queries, adds, failed_adds, connects, disconnects;
} stats = {0, 0, 0};
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
  cache_size = 1024e6;
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
    oflags = DB_CREATE | DB_THREAD | DB_AUTO_COMMIT | DB_READ_UNCOMMITTED;

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
  struct rec_val *v = (struct rec_val *)buf;
  struct rec_key new_k;
  struct rec_val new_v;
  
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
  new_v.n_valid = 0;
  new_v.period_length = bucket_sizes[new_bucket_idx];
  new_v.tail_timestamp = 0;
  data_start_idx = 0;
  info("Spliting up %i records\n", v->n_valid);
  for (i = 0; i <= v->n_valid; i++) {
    if (i < v->n_valid &&
        v->data[i].timestamp < new_k.timestamp + new_v.period_length) {
    } else {
      /* commit the old data under the new key */
      new_v.n_valid = i - data_start_idx;
      new_v.tail_timestamp = v->data[i-1].timestamp;

      info("writing new bucket anchor 0x%x size %i nvalid %i idx: %i\n", 
           new_k.timestamp, new_v.period_length, new_v.n_valid, data_start_idx);
      debug(" extra info [%i] %i %i\n", i, new_v.tail_timestamp, v->data[i-1].timestamp);
      if (put(dbp, tid, &new_k, &new_v, sizeof(new_v)) < 0)
        return -1;

      if (i > data_start_idx) {
        if (put_partial(dbp, tid, &new_k, buf + POINT_OFF(data_start_idx), 
                        POINT_OFF(i) - POINT_OFF(data_start_idx),
                        sizeof(struct rec_val)) < 0)
          return -1;
      }

      if (i < v->n_valid) {
        /* start a new key */
        new_k.timestamp = v->data[i].timestamp - 
          (v->data[i].timestamp % bucket_sizes[new_bucket_idx]);
        new_v.n_valid = 0;
        new_v.tail_timestamp = 0;
        data_start_idx = i;
      }
    }
  }
  return 0;
}

int add(DB *dbp, struct ipc_command *c) {
  int cur_rec, ret;
  DBC *cursorp;
  struct rec_key cur_k;
  struct rec_val cur_v;
  DB_TXN *tid = NULL;

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

  for (cur_rec = 0; cur_rec < c->args.add.n; cur_rec++) {
    debug("Adding reading ts: 0x%x\n", c->args.add.v[cur_rec].timestamp);

    if (cur_v.n_valid > 0 &&
        cur_k.stream_id == c->streamid &&
        cur_k.timestamp <= c->args.add.v[cur_rec].timestamp &&
        cur_k.timestamp + cur_v.period_length > c->args.add.v[cur_rec].timestamp) {
      /* we're already in the right bucket; don't need to do anything */
      debug("In valid bucket.  n_valid: %i\n", cur_v.n_valid);
    } else {
      /* need to find the right bucket */
      debug("Looking up bucket\n");
      cur_k.stream_id = c->streamid;
      cur_k.timestamp = c->args.add.v[cur_rec].timestamp;

      if ((ret = get_bucket(cursorp, &cur_k, &cur_v)) <= 0) {
        /* create a new bucket */

        /* the key has been updated by get_bucket */
        cur_v.n_valid = 0;
        cur_v.period_length = bucket_sizes[-ret];
        cur_v.tail_timestamp = 0;
        debug("Created new bucket anchor: %i length: %i\n", cur_k.timestamp, cur_v.period_length);
      } else {
        debug("Found existing bucket streamid: %i anchor: %i length: %i\n", 
              cur_k.stream_id, cur_k.timestamp, cur_v.period_length);
      }
    }

    /* start the insert -- we're in the current bucket */
    if (cur_v.tail_timestamp < c->args.add.v[cur_rec].timestamp ||
        cur_v.n_valid == 0) {
      /* if it's an append or a new bucket we can just write the values */
      /* update the header block */
      cur_v.tail_timestamp = c->args.add.v[cur_rec].timestamp;
      cur_v.n_valid++;
      if (put_partial(dbp, tid, &cur_k, &cur_v, sizeof(cur_v), 0) < 0)
        goto abort;
      /* and the data */
      if (put_partial(dbp, tid, &cur_k, &c->args.add.v[cur_rec], sizeof(struct point),
                      POINT_OFF(cur_v.n_valid-1)) < 0)
        goto abort;
      debug("Append detected; inserting at offset: %i\n", POINT_OFF(cur_v.n_valid-1));
    } else {
      char buf[POINT_OFF(MAXBUCKETRECS + NBUCKETSIZES)];
      struct rec_val *v = (struct rec_val *)buf;
      int i;
      /* otherwise we have to insert it somewhere. we'll just read out
         all the data and do the insert stupidly. */
      if ((ret = get(cursorp, DB_SET | DB_RMW, &cur_k, buf, sizeof(buf))) < 0) {
        warn("error reading full bucket into memory (%i)!\n", ret);
        continue;
      }
      for (i = 0; i < v->n_valid; i++) {
        if (v->data[i].timestamp >= c->args.add.v[cur_rec].timestamp)
          break;
      }
      debug("Inserting within existing bucket index: %i (%i %i)\n", 
            i, c->args.add.v[cur_rec].timestamp, cur_v.tail_timestamp);
      /* appends should have been handled without reading back the whole record */
      assert(i < v->n_valid);
      // assert(i < SMALL_POINTS);
      /* we have our insert position */
      if (v->data[i].timestamp == c->args.add.v[cur_rec].timestamp) {
        /* replace a record */
        debug("Replacing record with timestamp 0x%x\n", c->args.add.v[cur_rec].timestamp);
        if (put_partial(dbp, tid, &cur_k, &c->args.add.v[cur_rec], sizeof(struct point),
                        POINT_OFF(i)) < 0)
          goto abort;
      } else {
        /* shift the existing records back */
        debug("Inserting new record\n");
        if (put_partial(dbp, tid, &cur_k, &v->data[i], 
                        (v->n_valid - i) * sizeof(struct point),
                        POINT_OFF(i+1)) < 0)
          goto abort;
        if (put_partial(dbp, tid, &cur_k, &c->args.add.v[cur_rec], 
                        sizeof(struct point),
                        POINT_OFF(i)) < 0) 
          goto abort;
      
        cur_v.n_valid++;
        /* and update the header */
        if (put_partial(dbp, tid, &cur_k, &cur_v, sizeof(struct rec_val), 0) < 0)
          goto abort;
      }
    }
    if (cur_v.n_valid > MAXBUCKETRECS) {
      info("Splitting buckets since this one is full!\n");
      if (split_bucket(dbp, cursorp, tid, &cur_k) < 0)
        goto abort;
      bzero(&cur_k, sizeof(cur_k));
      bzero(&cur_v, sizeof(cur_v));
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
    do_shutdown = 1;
  }
  return -1;
}

int add_enqueue(struct ipc_command *c) {
  unsigned long long key;
  struct ipc_command *points;;
  assert(c->command == COMMAND_ADD);

  debug("add_enqueue\n");

  if (pthread_mutex_lock(&dbs[c->dbid].lock) != 0)
    return -1;

  key = c->streamid;
  points = hashtable_search(dbs[c->dbid].dirty_data, &key);
  if (points == NULL) {
    unsigned long long *new_key = malloc(sizeof(unsigned long long));
    if (!new_key)
      goto fail;
    points = FREELIST_GET(struct ipc_command, dirty_data); 
    if (!points) {
      free(new_key);
      goto fail;
    }
    debug("creating new hashtable entry dbid: %i streamid: %i\n", 
          c->dbid, c->streamid);

    *new_key = c->streamid;
    memcpy(points, c, sizeof(struct ipc_command));
    points->args.add.n = 0;

    if (!hashtable_insert(dbs[c->dbid].dirty_data, new_key, points)) {
      free(new_key);
      FREELIST_PUT(struct ipc_command, dirty_data, points);
      goto fail;
    }
  }
  if (c->args.add.n > SMALL_POINTS - points->args.add.n) {
    /* do big adds directly */
    info("writing data directly: streamid: %li bucket: %i n: %i\n",
         key, points->args.add.n, c->args.add.n);
    pthread_mutex_unlock(&dbs[c->dbid].lock);

    if (add(dbs[c->dbid].dbp, c) < 0) {
      sleep(rand() % 1 );
      warn("Transaction aborted... retrying\n");
      if (add(dbs[c->dbid].dbp, c) < 0) {
        warn("Retry failed... giving up\n");
        INCR_STAT(failed_adds);
      }
    }
  } else {
    /* there's enough room to defer this add */
    memcpy(&points->args.add.v[points->args.add.n], c->args.add.v, 
           c->args.add.n * sizeof(struct point));
    points->args.add.n += c->args.add.n;
    debug("Added %i new records for deferred load\n", c->args.add.n);
    pthread_mutex_unlock(&dbs[c->dbid].lock);
    return 0;
  }
  return 0;
 fail:
  pthread_mutex_unlock(&dbs[c->dbid].lock);
  return -1;
}

void commit_data(struct config *conf) {
  int dbid = 0, done = 0;
  setitimer(ITIMER_PROF, &global_itimer, NULL);


  pthread_mutex_lock(&shutdown_lock);
  while (!done) {
    struct timespec sleep_time;
    clock_gettime(CLOCK_REALTIME, &sleep_time);
    sleep_time.tv_sec += conf->commit_interval;
    if (pthread_cond_timedwait(&shutdown_cond, &shutdown_lock, &sleep_time) == 0)
      done = 1;

    debug("commit: checking for new data\n");
    for (dbid = 0; dbid < MAX_SUBSTREAMS; dbid++) {
      unsigned long long *key = NULL;
      struct ipc_command *val;

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
              val->dbid, val->streamid, val->args.add.n);
        
        if (add(dbs[dbid].dbp, val) < 0) {
          warn("Transaction aborted in commit thread... retrying\n");
          sleep(rand() % 10 );
          if (add(dbs[dbid].dbp, val) < 0) {
            warn("Transaction retry failed in commit thread... giving up\n");
            INCR_STAT(failed_adds);
          }
        }
        FREELIST_PUT(struct ipc_command, dirty_data, val);
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

void query(DB *dbp, struct ipc_command *c, struct ipc_reply *r) {
  int ret;
  DBC *cursorp;
  struct rec_key k;
  struct rec_val v;
  unsigned long long starttime, endtime;
  int streamid;
  
  streamid = c->streamid;
  starttime = c->args.query.starttime;
  endtime = c->args.query.endtime;
  debug("starting query id: %i start: %i end: %i\n", streamid, starttime, endtime);

  /* set up the query key */
  k.stream_id = streamid;
  k.timestamp = starttime - (starttime % bucket_sizes[NBUCKETSIZES-1]);

  r->reply = REPLY_QUERY;
  r->data.query.nrecs = 0;

  ret = dbp->cursor(dbp, NULL, &cursorp, 0);
  if (cursorp == NULL) {
    dbp->err(dbp, ret, "cursor");
    return;
  }

  if (get_partial(cursorp, DB_SET_RANGE, &k, &v, sizeof(struct rec_val), 0) < 0) {
    goto done;
  }

  do {
    int start, i, first_rec = 0;
    int read_recs = min(v.n_valid, MAXRECS - r->data.query.nrecs);
    debug("examining record start: 0x%x length: %i streamid: %i\n", 
          k.timestamp, v.period_length, k.stream_id);
    if (streamid != k.stream_id) break;
    if (k.timestamp > endtime) break;
    if (r->data.query.nrecs > MAXRECS) break;

    /* holla.  we can read right into the result buffer */
    if (get_partial(cursorp, DB_SET, &k, &r->data.query.pts[r->data.query.nrecs],
                sizeof(struct point) * read_recs,
		    sizeof(struct rec_val)) < 0) {
      goto next;
    }
    start = r->data.query.nrecs;
    for (i = start; i < start + read_recs; i++) {
      if (r->data.query.pts[i].timestamp >= starttime &&
          r->data.query.pts[i].timestamp < endtime) {
        r->data.query.nrecs ++;
      } else if (r->data.query.pts[i].timestamp < starttime) {
        first_rec = i + 1;
      }
    }
    debug("query: added %i/%i records\n", r->data.query.nrecs, v.n_valid);
    if (first_rec > 0) {
      memmove(&r->data.query.pts[0], &r->data.query.pts[first_rec], 
              r->data.query.nrecs * sizeof(struct point));
    }
  next:
    ;
  } while (get_partial(cursorp, DB_NEXT, &k, &v, 
                       sizeof(struct rec_val), 0) == 0);

  r->data.query.nrecs = (r->data.query.nrecs > MAXRECS) ? MAXRECS : r->data.query.nrecs;
  debug("returning %i records\n", r->data.query.nrecs);
  r->reply = REPLY_OK;
  
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
          "\t\t-p <port>          local port to bind to\n\n",
          progname);
}

int optparse(int argc, char **argv, struct config *c) {
  char o;
  char *endptr;
  while ((o = getopt(argc, argv, "vhd:c:p:")) != -1) {
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

void *process_request(void *request) {
  char buf[4096];
  struct sock_request *req = (struct sock_request *)request;
  struct timeval timeout;
  FILE *fp = NULL;
  int dbid = 0;
  struct ipc_command cmd;
  memset(&cmd, 0, sizeof(cmd));
  
  timeout.tv_sec = 60;
  timeout.tv_usec = 0;
  
  if (setsockopt(req->sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0)
    goto done;

  fp = fdopen(req->sock, "r+");
  if (!fp)
    goto done;

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
      if (cmd.command == COMMAND_ADD) {
        add_enqueue(&cmd);
        memset(&cmd, 0, sizeof(cmd));
      }
    } else if (memcmp(buf, "put", 3) == 0) {
      int streamid, timestamp, sequence, converted, idx;
      double value, min = LLONG_MIN, max = LLONG_MAX;

      converted = sscanf(buf, "put %i %i %i %lf %lf %lf", 
                         &streamid, &timestamp, &sequence,
                         &value, &min, &max);
      if (converted < 3) {
        char *msg = "-2 invalid argument\n";
        fwrite(msg, strlen(msg), 1, fp);
        goto done;
      }

      if (cmd.command != COMMAND_ADD) {
        cmd.args.add.n = 0;
      } else {
        if (cmd.dbid != dbid ||
            cmd.streamid != streamid) {
          add_enqueue(&cmd);
          cmd.args.add.n = 0;
        }
      }
      cmd.command = COMMAND_ADD;
      cmd.dbid = dbid;
      cmd.streamid = streamid;
          
      idx = cmd.args.add.n++;
      cmd.args.add.v[idx].timestamp = timestamp;
      cmd.args.add.v[idx].reading_sequence = sequence;
      cmd.args.add.v[idx].reading = value;
      cmd.args.add.v[idx].min = min;
      cmd.args.add.v[idx].max = max;

      if (cmd.args.add.n == SMALL_POINTS) {
        add_enqueue(&cmd);
        memset(&cmd, 0, sizeof(cmd));
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
      if (cmd.command == COMMAND_ADD) {
        add_enqueue(&cmd);
      }

      cmd.command = COMMAND_QUERY;
      cmd.dbid = dbid;
      cmd.streamid = streamid;
      cmd.args.query.starttime = start;
      cmd.args.query.endtime = end;

      query(dbs[dbid].dbp, &cmd, r);

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
    }
  }
  
  if (cmd.command == COMMAND_ADD &&
      cmd.args.add.n > 0) {
    add_enqueue(&cmd);
  }
  
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
  if (c->commit_interval > 0) {
    pthread_create(thread, NULL, (void *)(void *)commit_data, c);
    pthread_detach(*thread);
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

  log_setlevel(conf.loglevel);

  drop_priv();

  // open the database
  db_open(&conf);
  
  signal(SIGINT, sig_shutdown);
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
  return 0;
}
