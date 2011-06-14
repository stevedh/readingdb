
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <string.h>
#include <errno.h>

#include "logging.h"
#include "stats.h"
#include "util.h"

static short sock = -1;
struct pending_stat {
  char *s_name;
  struct timeval s_time;
};

void stats_init(short p) {
  struct sockaddr_in6 dest = {
    .sin6_family = AF_INET6,
    .sin6_addr = IN6ADDR_LOOPBACK_INIT,
    .sin6_port = htons(p),
  };
  sock = socket(AF_INET6, SOCK_DGRAM, 0);
  if (sock < 0) {
    warn("stats_init: socket: %s\n", strerror(errno));
    return;
  }

  if (connect(sock, (struct sockaddr *)&dest, sizeof(dest)) < 0) {
    warn("stats_init: connect: %s\n", strerror(errno));
    return;
  }
}

void *stats_tic(char *stat) {
  struct pending_stat *b = malloc(sizeof(struct pending_stat));
  if (!b) return NULL;
  b->s_name = stat;
  gettimeofday(&b->s_time, NULL);
  return b;
}

void stats_toc(void *p) {
  float d;
  struct timeval now, delta;
  struct pending_stat *pending_stat = p;
  gettimeofday(&now, NULL);
  timeval_subtract(&delta, &now, &pending_stat->s_time);
  d = delta.tv_sec + (((float)delta.tv_usec) / 1e6);
  free(p);

  
}

void stats_report(struct stats *s, struct timeval *when) {
  char buf[1024];
  int n;

  if (sock < 0) {
    return;
  }

  n = snprintf(buf, sizeof(buf),
               "{\"timestamp\": %li.%06li, "
               "\"queries\": %i, \"adds\": %i, "
               "\"failed_adds\": %i, "
               "\"connects\": %i, \"disconnects\": %i,"
               "\"nearest\": %i }\n",
               when->tv_sec, when->tv_usec,
               s->queries, s->adds, s->failed_adds, 
               s->connects, s->disconnects,
               s->nearest);

  if (send(sock, buf, n, MSG_DONTWAIT) < 0) {
    debug("stats_report: send: %s\n", strerror(errno));
  }
}

void stats_close() {
  close(sock);
}
