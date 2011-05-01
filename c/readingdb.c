
#include <Python.h>
#include <stdlib.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <db.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <signal.h>

#include "readingdb.h"
#include "readingdb_py.h"
#include "util.h"

#define TIMEOUT_SECS 10
#define SIGREPLACE SIGTERM

int call_start(IPC *ipp) {
  struct timespec ts;
  struct sigaction act;

  if (ipp == NULL)
    return -1;
  /* abort if we can't lock ourselves fast enough */
  /* however, after we get this lock we must finish the protocol... */
  if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
    PyErr_SetString(PyExc_IOError, "clock_gettime failed");
    return -1;
  }
  ts.tv_sec += TIMEOUT_SECS;

  /* check if SA_RESTART is set on the SIGREPLACE handler.  This could
     cause us to hang on exit */
  ipp->sa_flags = 0;
  if (sigaction(SIGREPLACE, NULL, &act) < 0)
    return -1;
  if (act.sa_flags & SA_RESTART) {
    ipp->sa_flags = act.sa_flags;
    act.sa_flags &= ~SA_RESTART;
    if (sigaction(SIGREPLACE, &act, NULL) < 0) {
      ipp->sa_flags = 0;
      return -1;
    }
  }

  if (sem_timedwait(ipp->mutex_caller, &ts) < 0) {
    act.sa_flags = ipp->sa_flags;
    sigaction(SIGREPLACE, &act, NULL);
    return -1;
  }

  return 0;
}

int call_end(IPC *ipp) {
  struct sigaction act;
  while (sem_post(ipp->mutex_caller) < 0) {
    fprintf(stderr, "sem_post returned in call_end\n");
  }

  if (ipp->sa_flags & SA_RESTART) {
    /* *shrug* */
    if (sigaction(SIGREPLACE, NULL, &act) < 0) return -1;
    act.sa_flags = ipp->sa_flags;
    sigaction(SIGREPLACE, &act, NULL);
  }
  return 0;
}

IPC *db_open(void) {
  IPC *ipp = ipc_open(0, O_APPEND);
  if (ipp == NULL) {
    PyErr_SetString(PyExc_IOError, strerror(errno));
  }
  return ipp;
}

void db_substream(IPC *ipp, int substream) {
  ipp->substream = substream;
}

void db_close(IPC *ipp) {
  if (ipp) {
    ipc_close(ipp);
    free(ipp);
  }  
}

int db_sync(IPC *ipp) {
  struct ipc_command *c = ipp->shm;
  struct ipc_reply *r = ipp->shm;
  int rv = 0;

  if (call_start(ipp) < 0) {
    return -1;
  }

  c->command = COMMAND_SYNC;
  c->dbid = ipp->dbid;
  sem_post(ipp->mutex_server);
  while (sem_wait(ipp->mutex_reply) < 0);
  if (r->reply == REPLY_OK) {
    rv = -1;
    PyErr_Format(PyExc_IOError, "Error during query: %i", r->data.error);
  }
  call_end(ipp);
  return rv;
}

int db_add(IPC *ipp, int streamid,
           PyObject *values) {
  struct ipc_command *c = ipp->shm;
  struct ipc_reply *r = ipp->shm;
  int i, rv = 0;
  
  if (!PyList_Check(values)) {
    return -1;
  }

  if (PyList_Size(values) > MAXQUERYSET) {
    return -1;
  }

  if (call_start(ipp) < 0) 
    return -1;

  c->command = COMMAND_ADD;
  c->dbid = ipp->dbid;
  c->streamid = streamid;
  c->args.add.n = PyList_Size(values);

  for (i = 0; i < c->args.add.n; i++) {
    PyObject *tuple = PyList_GetItem(values, i);

    if (PyArg_ParseTuple(tuple, "iiddd", 
                         &c->args.add.v[i].timestamp,
                         &c->args.add.v[i].reading_sequence,
                         &c->args.add.v[i].reading,
                         &c->args.add.v[i].min,
                         &c->args.add.v[i].max) < 0) {
      c->args.add.n = i;
      break;
    }
  }

  // printf("addinging ts: %i %f\n", readingtime, reading);

  sem_post(ipp->mutex_server);
  while (sem_wait(ipp->mutex_reply) < 0);
  if (r->reply == REPLY_OK) {
    rv = -1;
    PyErr_Format(PyExc_IOError, "Error during query: %i", r->data.error);
  }
  call_end(ipp);
  return rv;
}

PyObject *db_query(IPC *ipp, unsigned long long streamid, 
                   unsigned long long starttime, 
                   unsigned long long endtime) {
  struct ipc_command *c = ipp->shm;
  struct ipc_reply *r = ipp->shm;
  PyObject *ret = NULL, *record;
  int i;

  if (call_start(ipp) < 0)
    return NULL;

  c->command = COMMAND_QUERY;
  c->dbid = ipp->dbid;
  c->streamid = streamid;
  c->args.query.starttime = starttime;
  c->args.query.endtime = endtime;

  sem_post(ipp->mutex_server);
  while (sem_wait(ipp->mutex_reply) < 0);

  if (r->reply == REPLY_ERROR) {
    PyErr_Format(PyExc_IOError, "Error during query: %i", r->data.error);
    ret = NULL;
  } else if (r->reply == REPLY_QUERY) {
    ret = PyList_New(r->data.query.nrecs);
    if (ret == NULL) {
      PyErr_SetString(PyExc_IOError, "couldn't allocate list");
      return NULL;
    }
    for (i = 0; i < r->data.query.nrecs; i++) {
      record = Py_BuildValue("iiddd", r->data.query.pts[i].timestamp,
                             r->data.query.pts[i].reading_sequence,
                             r->data.query.pts[i].reading,
                             r->data.query.pts[i].min,
                             r->data.query.pts[i].max);
      PyList_SetItem(ret, i, record);
    }
  }
  call_end(ipp);
  return ret;
}
