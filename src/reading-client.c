#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <semaphore.h>
#include <string.h>
#include <signal.h>
#include <time.h>

#include "readingdb.h"
#include "util.h"


int main() {
  IPC *ipp = ipc_open(0, 0);
  struct ipc_command *c = ipp->shm;
  struct ipc_reply *r = ipp->shm;
  int i;

  sem_wait(ipp->mutex_caller);
  printf("GOT CALLER\n");
  c->command = COMMAND_SYNC;
  sem_post(ipp->mutex_server);
  sem_wait(ipp->mutex_reply);
  sem_post(ipp->mutex_caller);

  // try querying
  sem_wait(ipp->mutex_caller);
  c->command = COMMAND_QUERY;
  c->args.query.streamid = 320;
  c->args.query.starttime = 0;
  c->args.query.endtime = time(NULL);
  
  sem_post(ipp->mutex_server);
  sem_wait(ipp->mutex_reply);
  printf("got reply: %i records\n", r->data.query.nrecs);
  for (i = 0; i < r->data.query.nrecs; i++) {
    printf("%i %f\n", r->data.query.pts[i].timestamp,
           r->data.query.pts[i].reading);
  }
  sem_post(ipp->mutex_caller);
  ipc_close(ipp);
  return 0;
}
