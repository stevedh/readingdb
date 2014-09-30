#ifndef STATS_H_
#define STATS_H_

#include <sys/time.h>

struct stats {
  int queries, adds, failed_adds, connects, disconnects, nearest, deletes;
};
#define INCR_STAT(STAT) { pthread_mutex_lock(&stats_lock);     \
    stats.STAT ++;                                             \
    pthread_mutex_unlock(&stats_lock); }

/* open the socket once */
void stats_init(unsigned short port);

/* report the statistics to any listener */
void stats_report(struct stats *s, struct timeval *ts);

/* exit */
void stats_close();

extern pthread_mutex_t stats_lock;
extern struct stats stats;

#endif
