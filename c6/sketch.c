
#include <stdlib.h>
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

sig_atomic_t do_shutdown = 0;
pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;
struct stats stats = {0};
extern struct subdb dbs[MAX_SUBSTREAMS];



enum {
  S_COUNT = 0,
  S_MEAN = 1,
  S_MIN = 2,
  S_MAX = 3,
};

ReadingSet **w_stats(ReadingSet *window, 
                   unsigned long long start, 
                   unsigned long long end,
                   int windowlen) {
  ReadingSet **rv = malloc(sizeof(ReadingSet *) * 4);
  int i = 0, window_idx = 0, j;
  unsigned long long current;
  
  for (j = 0; j < 4; j++) {
    rv[j] = _rpc_alloc_rs((end - start) / windowlen);
    rv[j]->n_data = 0;
  }

  /* iterate over the sub windows */
  for (current = start; current < end && i < window->n_data; current += windowlen) {
    int count = 0;
    double sum, min = INFINITY, max = -INFINITY;

    for (; window->data[i]->timestamp >= current &&
           window->data[i]->timestamp < current + windowlen &&
           i < window->n_data;
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
      rv[S_COUNT]->data[window_idx]->timestamp = current;
      rv[S_COUNT]->data[window_idx]->value = count;

      rv[S_MEAN]->data[window_idx]->timestamp = current;
      rv[S_MEAN]->data[window_idx]->value = (sum / count);

      rv[S_MAX]->data[window_idx]->timestamp = current;
      rv[S_MAX]->data[window_idx]->value = max;

      rv[S_MIN]->data[window_idx]->timestamp = current;
      rv[S_MIN]->data[window_idx]->value = min;

      window_idx ++;
    }
  };
  rv[S_COUNT]->n_data = window_idx;
  rv[S_MEAN]->n_data = window_idx;
  rv[S_MAX]->n_data = window_idx;
  rv[S_MIN]->n_data = window_idx;
  return rv;
}


/* sketch definitions.
 *
 * These must be sorted smallest to largest, and all window sizes must
 * divide the largest window size.  The results will be placed into
 * the substream corresponding to the sketch definition index in this
 * array.
 */
struct sketch {
  /* period of sketch computation, seconds */
  unsigned long int period;
  /* function that computes the window */
  /* a window function gets passed a readingset, start, and end value */
  /* it should return a new ReadingSet of window values it wants to update */
  ReadingSet** (*windowfn)(ReadingSet *window, 
                          unsigned long long start, 
                          unsigned long long end,
                          int windowlen);
  int nsubstreams;
} sketches [] = {
  { 300, w_stats, 4},           /* returns count, mean, min, max */
  { 3600, w_stats, 4},
};


// update the sketches for a streamid in the provided window
void update_sketches(int streamid, unsigned int start, unsigned int end) {
  Query q = QUERY__INIT;
  Response r = RESPONSE__INIT;
  unsigned long int current;
  unsigned long int fetch_period = 3600;
  int i, j;

  r.data = _rpc_alloc_rs(MAXRECS);
  info("r.data: %p\n", r.data);
  info("Updating sketch for stream %i\n", streamid);
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
    for (i = 0; i < sizeof(sketches) / sizeof(sketches[0]); i++) {
      ReadingSet **rv = 
        sketches[i].windowfn(r.data, current, current + fetch_period, sketches[i].period);;
      for (j = 0; j < sketches[i].nsubstreams; j++) {
        if (rv[j] && rv[j]->n_data) {
          info("got %i records from filter, %i %i\n", rv[j]->n_data, cursubstream, j);
          add(dbs[cursubstream].dbp, rv[j]);
        }
        if (rv[j]) {
          _rpc_free_rs(rv[j]);
        }
        cursubstream ++;
      }
    }
    
  };
  _rpc_free_rs(r.data);
}


int main(int argc, char **argv) {
  struct config c;
  c.cache_size = 100;
  strcpy(c.data_dir, "testdata");

  log_init();
  log_setlevel(LOGLVL_INFO);

  db_open(&c);

  //update_sketches(5, time(NULL) - (3600 * 24 *1000), time(NULL));
  update_sketches(5, 1405447200, 1405468800);
  db_close();
}
