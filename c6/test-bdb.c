
#include <stdlib.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <db.h>

#include "readingdb.h"

int
timeval_subtract (result, x, y)
struct timeval *result, *x, *y;
{
  /* Perform the carry for the later subtraction by updating y. */
  if (x->tv_usec < y->tv_usec) {
    int nsec = (y->tv_usec - x->tv_usec) / 1000000 + 1;
    y->tv_usec -= 1000000 * nsec;
    y->tv_sec += nsec;
  }
  if (x->tv_usec - y->tv_usec > 1000000) {
    int nsec = (x->tv_usec - y->tv_usec) / 1000000;
    y->tv_usec += 1000000 * nsec;
    y->tv_sec -= nsec;
  }
     
  /* Compute the time remaining to wait.
     tv_usec is certainly positive. */
  result->tv_sec = x->tv_sec - y->tv_sec;
  result->tv_usec = x->tv_usec - y->tv_usec;
     
  /* Return 1 if result is negative. */
  return x->tv_sec < y->tv_sec;
}

#define DATABASE "readings.bdb"

int main() {
  DB *dbp;
  int ret, i, j;
  int start_time = time(NULL);
  struct timeval start, end, delta;

  if ((ret = db_create(&dbp, NULL, 0)) != 0) {
    fprintf(stderr, "db_create: %s\n", db_strerror(ret));
    exit(1);
  }

  if ((ret = dbp->open(dbp, 
                       NULL, DATABASE, NULL, DB_BTREE, DB_CREATE, 0644)) != 0) {
    dbp->err(dbp, ret, "%s", DATABASE);
    exit(1);
  }

  
  for (i = 0; i < 3 * 60 * 24 * 10; i++) {
    gettimeofday(&start, NULL);
    for (j = 0; j < 2000; j++) {
      DBT key, data;
      struct rec_key k;
      struct rec_val v;
      memset(&key, 0, sizeof(key));
      memset(&data, 0, sizeof(data));

      k.stream_id = htonl(j);
      k.timestamp = htonl(start_time);
      v.reading_sequence = i;
      v.reading = (float)rand();
      key.data = &k;
      key.size = sizeof(k);
      data.data = &v;
      data.size = sizeof(v);
      ret = dbp->put(dbp, NULL, &key, &data, DB_NOOVERWRITE);
      if (ret == DB_KEYEXIST) {
        dbp->err(dbp, ret, "Put failed due to duplicate key");
      }
    }
    start_time += 20;
    if (i % 100 == 0)
      dbp->sync(dbp, 0);
    gettimeofday(&end, NULL);
    timeval_subtract(&delta, &end, &start);
    fprintf(stderr, "%i.%0.6i\n", delta.tv_sec, delta.tv_usec);
  }
  dbp->close(dbp, 0);
}
