
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <semaphore.h>
#include <string.h>
#include <signal.h>
#include <pwd.h>
#include <string.h>
#include <errno.h>
#include <db.h>
#include <arpa/inet.h>

#include "readingdb.h"
#include "logging.h"
#include "config.h"

#if 0
IPC *ipc_open(int semflags, int memflags) {
  IPC *ipp = malloc(sizeof(IPC));
  memset(ipp, 0, sizeof(IPC));
  char buf[4096];
  int written_len = 0;

  // FILE *log = stdout;  

  //fprintf(log, "logging\n");
  if (semflags & O_CREAT) {
    sem_unlink(SEM_NAME "CALLER");
    sem_unlink(SEM_NAME "SERVER");
    sem_unlink(SEM_NAME "REPLY");

    ipp->mutex_caller = sem_open(SEM_NAME "CALLER", semflags, 0666, 1);
    ipp->mutex_server = sem_open(SEM_NAME "SERVER", semflags, 0666, 0);
    ipp->mutex_reply = sem_open(SEM_NAME "REPLY", semflags, 0666, 0);
  } else {
    ipp->mutex_caller = sem_open(SEM_NAME "CALLER", semflags);
    ipp->mutex_server = sem_open(SEM_NAME "SERVER", semflags);
    ipp->mutex_reply = sem_open(SEM_NAME "REPLY", semflags);
  }
  if (ipp->mutex_caller == SEM_FAILED || 
      ipp->mutex_server == SEM_FAILED || 
      ipp->mutex_reply == SEM_FAILED) {
    error("Open semaphore failed: %s: %s\n", SEM_NAME, strerror(errno));
    goto sem_fail;
  }

  ipp->fd = open(SHM_PATH, memflags | O_RDWR, 0600);
  if (ipp->fd < 0) {
    error("Error opening shared file: %s: %s\n", SHM_PATH, strerror(errno));
    goto sem_fail;
  }

  if (memflags & O_CREAT) {
    memset(buf, 0, sizeof(buf));
    while (written_len < SHM_SIZ) {
      write(ipp->fd, buf, sizeof(buf));
      written_len += sizeof(buf);
    }
  }
  lseek(ipp->fd, 0, SEEK_SET);

  ipp->shm = mmap(NULL, SHM_SIZ, PROT_READ | PROT_WRITE, MAP_SHARED, ipp->fd, 0);
  if (ipp->shm == MAP_FAILED) {
    error("mmap: %s\n", strerror(errno));
    goto sem_fail;
  }
  close(ipp->fd);
  ipp->dbid = 0;
  return ipp;

 sem_fail:
  //fprintf(log, "FAILURE CONDITION\n");
  free(ipp);
  //fprintf(log, "DONE\n");
/*   SEM_DESTROY(ipp->mutex_caller, "CALLER") */
/*   SEM_DESTROY(ipp->mutex_server, "SERVER") */
/*   SEM_DESTROY(ipp->mutex_reply, "REPLY") */
  // fclose(log);
  return NULL;
}

void ipc_close(IPC *ipp) {
  sem_close(ipp->mutex_caller);
  sem_close(ipp->mutex_server);
  sem_close(ipp->mutex_reply);
  munmap(ipp->shm, SHM_SIZ);
}
#endif

void drop_priv(void) {
  const char *user = READINGDB_USER;
  struct passwd *user_pwd = getpwnam(user);
  info("trying to drop privileges to '%s'\n", user);
  if (user_pwd != NULL) {
    if (setuid(user_pwd->pw_uid) < 0) {
      error("setuid: %s\n", strerror(errno));
      return;
    }
    setgid(user_pwd->pw_gid);
  } else {
    error("no such user '%s'\n", user);
  }
}

int timeval_subtract(struct timeval *result, 
                     struct timeval *x, 
                     struct timeval *y) {
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

print_buf(void *buf, int len) {
  int i;
  uint8_t *cur = buf;
  printf("----\n  ");
  for (i = 0; i < len; i++) {
    printf("0x%02x ", cur[i]);
    if (i % 16 == 0 && i > 0)
      printf("\n  ");
  }
  printf("\nbuffer[%i]\n\n", len);
}
