
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <sys/file.h>

#include "stats.h"
#include "rdb.h"
#include "rpc.h"
#include "pbuf/rdb.pb-c.h"
#include "commands.h"
#include "sketch.h"
#include "intervals.h"
#include "config.h"

sig_atomic_t do_shutdown = 0;
pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;
struct stats stats = {0};
extern struct subdb dbs[MAX_SUBSTREAMS];
extern DB_ENV *env;


/*
 * Return a list of computed stats over a window.
 */
ReadingSet **w_stats(ReadingSet *window, 
                   unsigned long long start, 
                   unsigned long long end,
                   int windowlen) {
  ReadingSet **rv = malloc(sizeof(ReadingSet *) * 4);
  int i = 0, window_idx = 0, j;
  unsigned long long current;
  
  for (j = 0; j < 4; j++) {
    rv[j] = _rpc_alloc_rs(((end - start) / windowlen) + 1);
    rv[j]->n_data = 0;
  }

  /* iterate over the sub windows */
  for (current = start; current < end && i < window->n_data; current += windowlen) {
    int count = 0;
    double sum = 0., min = INFINITY, max = -INFINITY;

    for (; i < window->n_data && 
           window->data[i]->timestamp >= current &&
           window->data[i]->timestamp < current + windowlen;
         i++) {
      sum += window->data[i]->value;
      count += 1;
      if (window->data[i]->value < min) {
        min = window->data[i]->value;
      }
      if (window->data[i]->value > max) {
        max = window->data[i]->value;
      }
      i += 1;
    };

    if (count > 0) {
      rv[SKETCH__SKETCH_TYPE__COUNT-1]->data[window_idx]->timestamp = current;
      rv[SKETCH__SKETCH_TYPE__COUNT-1]->data[window_idx]->value = count;

      rv[SKETCH__SKETCH_TYPE__MEAN-1]->data[window_idx]->timestamp = current;
      rv[SKETCH__SKETCH_TYPE__MEAN-1]->data[window_idx]->value = (sum / count);

      rv[SKETCH__SKETCH_TYPE__MAX-1]->data[window_idx]->timestamp = current;
      rv[SKETCH__SKETCH_TYPE__MAX-1]->data[window_idx]->value = max;

      rv[SKETCH__SKETCH_TYPE__MIN-1]->data[window_idx]->timestamp = current;
      rv[SKETCH__SKETCH_TYPE__MIN-1]->data[window_idx]->value = min;

      window_idx ++;
    }
  };
  rv[SKETCH__SKETCH_TYPE__COUNT-1]->n_data = 0;// window_idx;
  rv[SKETCH__SKETCH_TYPE__MEAN-1]->n_data = window_idx;
  rv[SKETCH__SKETCH_TYPE__MAX-1]->n_data = 0;//window_idx;
  rv[SKETCH__SKETCH_TYPE__MIN-1]->n_data = 0;//window_idx;
  return rv;
}


/* 
 * Update the sketches for a streamid in the provided window.
 */
void update_sketches(struct config *c, 
                     unsigned int streamid, 
                     unsigned int start, 
                     unsigned int end) {
  Query q = QUERY__INIT;
  Response r = RESPONSE__INIT;
  unsigned long int current;
  unsigned long int fetch_period = 3600; /* SDH : this should be computed from the sketches list */
  int i, j;

  r.data = _rpc_alloc_rs(MAXRECS);
  memset(&q, 0, sizeof(q));
  q.streamid = streamid;
  q.substream = 0;

  /* iterate over our fetch chunks */
  for (current = start - (start % fetch_period); current <= end; current += fetch_period) {
    int cursubstream = 1;
    /* pull the data */
    r.data->n_data = 0;
    q.starttime = current;
    q.endtime = current + fetch_period;
    query(dbs[0].dbp, &q, &r, QUERY_DATA); 
    info("found %i records\n", r.data->n_data);

    /* iterate over the sketches we maintain */
    for (i = 0; i < 3; i++) {
      ReadingSet **rv = 
        w_stats(r.data, current, current + fetch_period, sketches[i].period);
      /* add the substreams back as data in the right substream*/
      for (j = 0; j < sketches[i].nsubstreams; j++) {
        if (rv[j] && rv[j]->n_data) {
          info("got %i records from filter, %i %i\n", rv[j]->n_data, cursubstream, j);
          rv[j]->streamid = streamid;
          rv[j]->substream = cursubstream;
          if (add(c, dbs[cursubstream].dbp, rv[j]) < 0) {
            warn("adding data failed...\n");
          }
        }
        if (rv[j]) {
          _rpc_free_rs(rv[j]);
        }
        cursubstream ++;
        assert(cursubstream < MAX_SUBSTREAMS);
      }
      free(rv);
    }
  }
  for (i = 1; i < MAX_SUBSTREAMS; i ++) {
    dbs[i].dbp->sync(dbs[i].dbp, 0);
  }
  _rpc_free_rs(r.data);
}

void usage(char *progname) {
  fprintf(stderr, 
          "\n\t%s [options]\n"
	  "\t\t-V                 print version and exit\n"
          "\t\t-v                 verbose\n"
          "\t\t-h                 help\n"
          "\t\t-s <cache size>    cache size (32MB)\n"
          "\t\t-d <datadir>       set data directory (%s)\n\n",
          progname, DATA_DIR);
}

void default_config(struct config *c) {
  char *cur;
  c->loglevel = LOGLVL_INFO;

  strcpy(c->data_dir, DATA_DIR);
  cur = stpncpy(c->sketch_log, c->data_dir, sizeof(c->sketch_log) - 1);
  *cur++ = '/';
  stpncpy(cur, DIRTY_SKETCH_LOGFILE, 
          sizeof(c->sketch_log) - (cur - c->sketch_log));

  c->cache_size = 32;
  c->sketch = 0;
}

int optparse(int argc, char **argv, struct config *c) {
  char o;
  char *endptr;
  while ((o = getopt(argc, argv, "Vvhd:s:")) != -1) {
    switch (o) {
    case 'h':
      usage(argv[0]);
      return -1;
      break;
    case 'V':
      printf("%s\n", PACKAGE_STRING);
      return -1;
    case 'v':
      c->loglevel = LOGLVL_DEBUG;
      break;
    case 'd':
      strncpy(c->data_dir, optarg, FILENAME_MAX);
      endptr = stpncpy(c->sketch_log, optarg, FILENAME_MAX);
      *endptr++ = '/';
      stpncpy(endptr, DIRTY_SKETCH_LOGFILE, sizeof(c->sketch_log) - (endptr - c->sketch_log));
      break;
    case 's':
      c->cache_size = strtol(optarg, &endptr, 10);
      if (endptr == optarg) {
        fatal("Invalid cache size\n");
        return -1;
      }
      break;
    }
  }
  return 0;
}


/*
 * Run to process the log file from reading-server, and recompute
 * sketches on dirty regions.
 *
 * Holds an advisory lock on the log file, so it safe to call multiple
 * times.
 */
int update_from_log(struct config *c) {
  FILE *lock_fp;
  char lockfile[1024], workfile[1024], *cur;
  struct stat sb;
  int ret, input_recs, merged_recs, i;
  unsigned int streamid, starttime, endtime;
  struct interval *input, *merged;

  /* 
     Locking protocol:

     1. Open the current work log and obtain an advisory lock on it.
     If this fails, some other process must be working so we exit.

     2. Check if there's an existing work file.  If there was, someone
     else must not have finished, so use that one to avoid loosing
     data.

     3. If there isn't one, the last guy must have finished
     successfully, so rename the current log to a workfile and process
     that.

     TODO:

     This would be made much more efficient by first computing the
     non-overlapping intervals and then iterating over those.
 */

  /* name the log file name */
  memcpy(workfile, c->sketch_log, sizeof(workfile));
  strcpy(workfile + strlen(c->sketch_log), ".work");
  info("Updating sketches from logfile %s\n", c->sketch_log);;

  memcpy(lockfile, c->sketch_log, sizeof(lockfile));
  strcpy(lockfile + strlen(c->sketch_log), ".lock");

  lock_fp = fopen(lockfile, "a");
  if (!lock_fp) {
    warn("Could not open logfile; cannot proceed\n");
    return -1;
  }

  if (flock(fileno(lock_fp), LOCK_EX | LOCK_NB) < 0) {
    warn("Log file is locked; aborting\n");
    return -1;
  }

  if (stat(workfile, &sb) == 0) {
    /* there's a work file already there (from a previous invocation?)
       so process it without copying it. */
    info("Work file exists, so proceeding using it\n");
  } else {
    info ("Copying log file to work file\n");
    if (rename(c->sketch_log, workfile) < 0) {
      error("Moving logfile failed: aborting: %s\n", strerror(errno));
      return -1;
    }
  }

  /* Workfile now contains the filename we want to process.  We will keep
     the logfile open to hold the write lock; we'll close that and
     release the lock on exit. */
  input = parse_file(workfile, &input_recs);
  if (!input) {
    error("Can't open work file (although it must exist?): %s\n", strerror(errno));
    return -1;
  }

  merged = merge_intervals(input, input_recs, &merged_recs);

  info("merged %i regions into %i\n", input_recs, merged_recs);

  for (i = 0; i < merged_recs; i++) {
    info("updating sketches for streamid: %i from: %i starttime to: %i: endtime\n", 
         merged[i].stream_id, merged[i].start, merged[i].end);
    update_sketches(c, merged[i].stream_id, merged[i].start, merged[i].end);
  }
  free(merged);
  free(input);

  if ((ret = env->txn_checkpoint(env, 10, 0, 0)) != 0) {
    warn("txn_checkpoint: %s\n", db_strerror(ret));
  }

  remove(workfile);
  return 0;
};


int main(int argc, char **argv) {
  struct config c;

  default_config(&c);

  if (optparse(argc, argv, &c) < 0) {
    exit(1);
  }

  drop_priv();

  log_init();
  log_setlevel(c.loglevel);

  db_open(&c);

  update_from_log(&c);

  db_close();
}
