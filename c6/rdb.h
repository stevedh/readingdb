#ifndef RDB_H_
#define RDB_H_

#include <db.h>

#include "pbuf/rdb.pb-c.h"
#include "logging.h"
#include "readingdb.h"

struct config {
  long commit_interval;          /* seconds */
  loglevel_t loglevel;
  char data_dir[FILENAME_MAX];
  unsigned short port;
  long cache_size;
  long deadlock_interval;
  long checkpoint_interval;
  int sketch;
  char sketch_log[FILENAME_MAX];
};

struct subdb {
  DB *dbp;
  char dbfile[128];

  /* dirty data */
  struct hashtable *dirty_data;
  pthread_mutex_t lock;
};

void db_open(struct config *conf);
void db_close();
int get_bucket(DBC *cursorp, struct rec_key *k, struct rec_val *v);
int split_bucket(DB *dbp, DBC *cursorp, DB_TXN *tid, struct rec_key *k);
int valid_bucketsize(int length);
int add(struct config *c, DB *dbp, ReadingSet *rs);
int add_enqueue(struct config *c, ReadingSet *rs, Response *reply);
void commit_data(struct config *conf);

void checkpoint_thread(void *p);
void deadlock_thread(void *p);
pthread_t **start_threads(struct config *c);
void stop_threads(pthread_t **threads);


#endif
