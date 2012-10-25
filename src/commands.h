#ifndef COMMANDS_H
#define COMMANDS_H

#include "pbuf/rdb.pb-c.h"

#define MAXRECS 10000 // (((MAXQUERYSET) < (IPC_MAX_RECORDS)) ? (MAXQUERYSET) : (IPC_MAX_RECORDS))
#define min(X,Y) ((X) < (Y) ? (X) : (Y))

enum query_action {
  QUERY_DATA = 1,
  QUERY_COUNT = 2,
};

/*
 * Conduct a range query of time-series data
 */
void query(DB *dbp, Query *q, Response *r, enum query_action action);

/*
 * Conduct a query relative to a reference time
 */
void query_nearest(DB *dbp, Nearest *n, Response *rs, enum query_action action);

/*
 * Delete data within a range for a stream
 */
int del(DB *dbp, Delete *d);

#endif
