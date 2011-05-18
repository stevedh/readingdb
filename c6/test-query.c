
#include <stdlib.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <db.h>

#include "readingdb.h"

#define DATABASE "readings.bdb"

int main(int argc, char **argv) {
  DB *dbp;
  int ret, i, j;
  int start_time = time(NULL);
  struct timeval start, end, delta;

  int dayplus = atoi(argv[1]);
  int stream = atoi(argv[2]);

  if ((ret = db_create(&dbp, NULL, 0)) != 0) {
    fprintf(stderr, "db_create: %s\n", db_strerror(ret));
    exit(1);
  }

  if ((ret = dbp->open(dbp, 
                       NULL, DATABASE, NULL, DB_BTREE, DB_CREATE, 0644)) != 0) {
    dbp->err(dbp, ret, "%s", DATABASE);
    exit(1);
  }

  DBT key, data;
  DBC *cursorp;
  struct rec_key k;
  struct rec_val v;
  memset(&key, 0, sizeof(key));
  memset(&data, 0, sizeof(data));
  
  printf("HERE\n");
  dbp->cursor(dbp, NULL, &cursorp, 0);
  i = 0;
  if ((ret = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
    k = *((struct rec_key *)key.data);
    v = *((struct rec_val *)data.data);
    printf("%i\t%i\t%i\n", ntohl(k.stream_id), ntohl(k.timestamp), v.reading);
  }
  dbp->err(dbp, ret, "cursor");
  k.stream_id = htonl(stream);
  k.timestamp = 0;//htons(ntohl(k.timestamp) + 3600 * 24 * dayplus);
  key.data = &k;
  key.size = sizeof(k);
  data.data = &v;
  data.size = sizeof(v);

  printf("searching stream: %i time: %i\n", ntohl(k.stream_id), ntohl(k.timestamp));
  ret = cursorp->get(cursorp, &key, &data, DB_SET_RANGE);
  while ((ret = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
    k = *((struct rec_key *)key.data);
    v = *((struct rec_val *)data.data);
    printf("%i\t%i\t%i\t%f\n", ntohl(k.stream_id), ntohl(k.timestamp), v.reading_sequence, v.reading);
    if (i++ > 500) break;
  }
  dbp->err(dbp, ret, "cursor");
  
  
  

  if ((ret = dbp->close(dbp, 0)) != 0) {
    dbp->err(dbp, ret, "%s", DATABASE);
    exit(1);
  }
}
