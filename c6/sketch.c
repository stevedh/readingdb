
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <math.h>

#include "stats.h"
#include "rdb.h"
#include "rpc.h"
#include "pbuf/rdb.pb-c.h"
#include "commands.h"
#include "sketch.h"
#include "config.h"

sig_atomic_t do_shutdown = 0;
pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;
struct stats stats = {0};
extern struct subdb dbs[MAX_SUBSTREAMS];
extern DB_ENV *env;

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
  rv[SKETCH__SKETCH_TYPE__COUNT-1]->n_data = window_idx;
  rv[SKETCH__SKETCH_TYPE__MEAN-1]->n_data = window_idx;
  rv[SKETCH__SKETCH_TYPE__MAX-1]->n_data = window_idx;
  rv[SKETCH__SKETCH_TYPE__MIN-1]->n_data = window_idx;
  return rv;
}


// update the sketches for a streamid in the provided window
void update_sketches(unsigned int streamid, unsigned int start, unsigned int end) {
  Query q = QUERY__INIT;
  Response r = RESPONSE__INIT;
  unsigned long int current;
  unsigned long int fetch_period = 3600; /* SDH : this should be compputed from the sketches list */
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
    debug("found %i records\n", r.data->n_data);

    /* iterate over the sketches we maintain */
    for (i = 0; i < 3; i++) {
      ReadingSet **rv = 
        w_stats(r.data, current, current + fetch_period, sketches[i].period);;
      /* add the substreams back as data in the right substream*/
      for (j = 0; j < sketches[i].nsubstreams; j++) {
        if (rv[j] && rv[j]->n_data) {
          debug("got %i records from filter, %i %i\n", rv[j]->n_data, cursubstream, j);
          rv[j]->streamid = streamid;
          rv[j]->substream = cursubstream;
          if (add(dbs[cursubstream].dbp, rv[j]) < 0) {
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
          "\t\t-v                 verbose\n"
          "\t\t-h                 help\n"
          "\t\t-d <datadir>       set data directory (%s)\n\n",
          progname, DATA_DIR);
}

void default_config(struct config *c) {
  c->loglevel = LOGLVL_INFO;
  strcpy(c->data_dir, DATA_DIR);
  c->cache_size = 100;
}

int optparse(int argc, char **argv, struct config *c) {
  char o;
  char *endptr;
  while ((o = getopt(argc, argv, "vhd:")) != -1) {
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
    }
  }
  return 0;
}

int update_from_log(struct config *c) {
  FILE *log_fp, *work_fp;
  char logfile[1024], workfile[1024], *cur;
  struct flock lock;
  struct stat sb;
  int ret;
  unsigned int streamid, starttime, endtime;

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
 */

  /* name the log file name */
  cur = logfile;
  cur = stpncpy(cur, c->data_dir, sizeof(logfile));
  *cur++ = '/';
  cur = stpncpy(cur, DIRTY_SKETCH_LOFILE, 20);

  memcpy(workfile, logfile, sizeof(workfile));
  strcpy(workfile + strlen(logfile), ".work");
  info("Updating sketches from logfile %s\n", logfile);;

  log_fp = fopen(logfile, "a");
  if (!log_fp) {
    warn("Could not open logfile; cannot proceed\n");
    return -1;
  }

  /* get an exclusive lock on the log */
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = 0;
  lock.l_len = 10;
  if (fcntl(fileno(log_fp), F_SETLK, &lock) < 0) {
    warn("Log file is locked by pid %i; aborting\n", lock.l_pid);
    return -1;
  }

  if (stat(workfile, &sb) == 0) {
    /* there's a work file already there (from a previous invocation?)
       so process it without copying it. */
    info("Work file exists, so proceeding using it\n");
  } else {
    info ("Copying log file to work file\n");
    if (rename(logfile, workfile) < 0) {
      error("Moving logfile failed: aborting: %s\n", strerror(errno));
      return -1;
    }
  }

  /* Workfile now contains the filename we want to process.  We will keep
     the logfile open to hold the write lock; we'll close that and
     release the lock on exit. */
  work_fp = fopen(workfile, "r");
  if (!work_fp) {
    error("Can't open work file (although it must exist?): %s\n", strerror(errno));
    return -1;
  }

  while (fscanf(work_fp, "%u\t%u\t%u\n", &streamid, &starttime, &endtime) == 3) {
    info("updating sketches for streamid: %i from: %i starttime to: %i: endtime\n", 
          streamid, starttime, endtime);
    update_sketches(streamid, starttime, endtime);

    if ((ret = env->txn_checkpoint(env, 10, 0, 0)) != 0) {
      warn("txn_checkpoint: %s\n", db_strerror(ret));
    }
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

  log_init();
  log_setlevel(c.loglevel);

  db_open(&c);

  update_from_log(&c);

  db_close();
}
