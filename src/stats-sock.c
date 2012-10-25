
#include <stdio.h>
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

static int sock = -1;

void stats_init(unsigned short p) {
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
