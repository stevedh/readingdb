#ifndef STATS_H_
#define STATS_H_

#include <sys/time.h>

struct stats {
  int queries, adds, failed_adds, connects, disconnects, nearest, deletes;
};

/* open the socket once */
void stats_init(unsigned short port);

/* report the statistics to any listener */
void stats_report(struct stats *s, struct timeval *ts);

/* exit */
void stats_close();

#endif
