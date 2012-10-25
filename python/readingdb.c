
#include <Python.h>
#include <numpy/arrayobject.h>
#include <stdlib.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "readingdb_py.h"

#include "c6/readingdb.h"
#include "c6/rpc.h"
#include "c6/commands.h"

#define TIMEOUT_SECS 10
#define SIGREPLACE SIGTERM


// version of db_open which can be called without holding the GIL
struct sock_request *__db_open(const char *host, const short port, int *rc) {
  struct sock_request *req = malloc(sizeof(struct sock_request));
  struct addrinfo *res, hints;
  struct sockaddr_in dest;
  struct timeval timeout;

  *rc = 0;

  if (!req) {
    *rc = ENOMEM;
    return NULL;
  }; 

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  if (getaddrinfo(host, NULL, &hints, &res) != 0) {
    *rc = ENOENT;
    return NULL;
  }

  memcpy(&dest, res->ai_addr, res->ai_addrlen);
  dest.sin_port = htons(port);
  freeaddrinfo(res);

  req->sock = socket(AF_INET, SOCK_STREAM, 0);
  if (req->sock < 0) {
    *rc = errno;
    goto cleanup;
  }

  timeout.tv_sec = 30;
  timeout.tv_usec = 0;
  if (setsockopt(req->sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
    *rc = errno;
    close(req->sock);
    goto cleanup;
  }

  if (connect(req->sock, (struct sockaddr *)&dest, sizeof(dest)) < 0) {
    *rc = errno;
    goto cleanup;
  }

  req->sock_fp = fdopen(req->sock, "r+");
  if (req->sock_fp == NULL) {
    *rc = errno;
    close(req->sock);
    goto cleanup;
  }
  req->substream = 0;
  return req;
 cleanup:
  free(req);
  return NULL;
}

// friendly db_open which raises python exceptions
struct sock_request *db_open(const char *host, const short port) {
  int rc;
  void *rv;
  rv = __db_open(host, port, &rc);
  if (rc == 0) {
    return rv;
  } else if (rc == ENOMEM) {
    return (struct sock_request *)PyErr_NoMemory();
  } else {
    PyErr_Format(PyExc_IOError, "db_open: %s", strerror(rc));
    return NULL;
  }
}

int db_add(struct sock_request *ipp, int streamid, PyObject *values) {
  int i, len;
  ReadingSet *r;
  struct pbuf_header h;
  unsigned char *buf;

  r = _rpc_alloc_rs(SMALL_POINTS);
  if (!r) {
    PyErr_SetNone(PyExc_MemoryError);
    return 0;
  }
  if (!ipp) {
    PyErr_SetString(PyExc_ValueError, "db_add: conn is NULL");
    return 0;
  }

  r->streamid = streamid;
  r->substream = ipp->substream;

  if (!PyList_Check(values)) {
    _rpc_free_rs(r);
    return 0;
  }

  if (PyList_Size(values) > SMALL_POINTS) {
    _rpc_free_rs(r);
    return 0;
  }

  for (i = 0; i < PyList_Size(values); i++) {
    PyObject *tuple = PyList_GetItem(values, i);
    reading__init(r->data[i]);
    if (PyTuple_Size(tuple) == 5) {
      if (PyArg_ParseTuple(tuple, "llddd",
                           &r->data[i]->timestamp,
                           &r->data[i]->seqno,
                           &r->data[i]->value,
                           &r->data[i]->min,
                           &r->data[i]->max) < 0)
        break;
      r->data[i]->has_min = 1;
      r->data[i]->has_max = 1;
    } else if (PyTuple_Size(tuple) == 3) {
      if (PyArg_ParseTuple(tuple, "lld",
                           &r->data[i]->timestamp,
                           &r->data[i]->seqno,
                           &r->data[i]->value) < 0)
        break;
    } else if (PyTuple_Size(tuple) == 2) {
      if (PyArg_ParseTuple(tuple, "ld",
                           &r->data[i]->timestamp,
                           &r->data[i]->value) < 0)
        break;
      r->data[i]->seqno = 0;
    } else {
      _rpc_free_rs(r);
      PyErr_Format(PyExc_ValueError, 
                   "Invalid data passed: must be a list of tuples");
      return 0;
    }
    if (r->data[i]->seqno != 0)
      r->data[i]->has_seqno = 1;
    r->n_data ++;
  }

  len = reading_set__get_packed_size(r);
  buf = malloc(len);
  if (!buf) {
    _rpc_free_rs(r);
    PyErr_SetNone(PyExc_MemoryError);
    return 0;
  }
  reading_set__pack(r, buf);
  _rpc_free_rs(r);

  h.body_length = htonl(len);
  h.message_type = htonl(MESSAGE_TYPE__READINGSET);
  
  if (fwrite(&h, sizeof(h), 1, ipp->sock_fp) <= 0) {
    free(buf);
    PyErr_Format(PyExc_IOError, "error writing to socket: %s", strerror(errno));
    return 0;
  }
  if (fwrite(buf, len, 1, ipp->sock_fp) <= 0) {
    free(buf);
    PyErr_Format(PyExc_IOError, "error writing to socket: %s", strerror(errno));
    return 0;
  }
  free(buf);
  return 1;
}

int db_query_all(struct sock_request *ipp, unsigned long long streamid, 
                       unsigned long long starttime, 
                       unsigned long long endtime,
                       enum query_action action) {
  Query q = QUERY__INIT; 
  struct pbuf_header h;
  unsigned char buf [512];
  int len;

  q.streamid = streamid;
  q.substream = ipp->substream;
  q.starttime = starttime;
  q.endtime = endtime;
  q.has_action = 1;
  q.action = action;

  if ((len = query__get_packed_size(&q)) < sizeof(buf)) {
    /* pack the request */
    query__pack(&q, buf);
    h.message_type = htonl(MESSAGE_TYPE__QUERY);
    h.body_length = htonl(len);
    /* send it */
    if (fwrite(&h, sizeof(h), 1, ipp->sock_fp) <= 0)
      goto write_error;
    if (fwrite(buf, len , 1, ipp->sock_fp) <= 0)
      goto write_error;
    fflush(ipp->sock_fp);

    return 0; 
  }
 write_error:
  return -errno;
}

int db_iter(struct sock_request *ipp, int streamid, 
            unsigned long long reference, int direction, int ret_n) {
  struct pbuf_header h;
  Nearest n = NEAREST__INIT;
  unsigned char buf[512];
  int len;

  n.streamid = streamid;
  n.reference = reference;
  n.direction = direction;

  if (ret_n > 1) {
    n.has_n = 1;
    n.n = ret_n;
  }

  if ((len = nearest__get_packed_size(&n)) > sizeof(buf)) {
    return -ENOMEM;
  }

  h.message_type = htonl(MESSAGE_TYPE__NEAREST);
  h.body_length = htonl(len);
  nearest__pack(&n, buf);
  
  if (fwrite(&h, sizeof(h), 1, ipp->sock_fp) <= 0)
    goto write_error;
  if (fwrite(buf, len, 1, ipp->sock_fp) <= 0)
    goto write_error;
  fflush(ipp->sock_fp);

  return 0;

 write_error:
  return -errno;
}


PyObject *db_query(unsigned long long *streamids, 
                   unsigned long long starttime, 
                   unsigned long long endtime,
                   int limit,
                   struct sock_request *ipp) {
  struct request_desc d;
  d.streamids = streamids;
  d.type = REQ_QUERY;
  d.starttime = starttime;
  d.endtime = endtime;
  d.limit = limit > 0 ? limit : 1e6;
  return db_multiple(ipp, &d);
}

PyObject *db_next(unsigned long long *streamids,
                  unsigned long long reference, int n,
                  struct sock_request *ipp) {
  struct request_desc d;
  d.streamids = streamids;
  d.type = REQ_ITER;
  d.starttime = reference;
  d.direction = NEAREST__DIRECTION__NEXT;
  d.limit = n > 0 ? n : 1;
  return db_multiple(ipp, &d);
  // return db_iter(ipp, streamid, reference, NEAREST__DIRECTION__NEXT, n);
}

PyObject *db_prev(unsigned long long *streamids,
                  unsigned long long reference, int n,
                  struct sock_request *ipp) {
  struct request_desc d;
  PyObject *numpy, *flipud, *p, *rv;
  int i;
  d.streamids = streamids;
  d.type = REQ_ITER;
  d.starttime = reference;
  d.direction = NEAREST__DIRECTION__PREV;
  d.limit = n > 0 ? n : 1;

  // load the data
  rv = db_multiple(ipp, &d);
  if (rv == NULL) {
    return NULL;
  }

  // use the numpy flipud to return a view on the data which has it in
  // the right order (ascending timestamps).
  numpy = PyImport_ImportModule("numpy");
  if (!numpy) {
    // ignore the import error
    PyErr_Clear();
    return rv;
  }
  flipud = PyObject_GetAttrString(numpy, "flipud");
  if (!flipud) {
    PyErr_Clear();
    Py_DECREF(numpy);
    return rv;
  }

  // try to flip all of the data vectors so they are ascending
  if (!PyList_Check(rv)) goto done;
  for (i = 0; i < PyList_Size(rv); i++) {
    p = PyList_GetItem(rv, i);
    PyList_SetItem(rv, i, PyObject_CallFunctionObjArgs(flipud, p, NULL));
    // the way I think this works is we call flipud, which creates a
    // "view" into the original array as a new object.  Since the
    // reference to the original array disappears, we don't need to
    // incref/decref it; we essentially donate our ref to the view.
  }
 done:
  Py_DECREF(numpy);
  Py_DECREF(flipud);
  return rv;
}

void db_del(struct sock_request *ipp, 
            unsigned long long streamid, 
            unsigned long long starttime, 
            unsigned long long endtime) {
  struct pbuf_header h;
  Delete d = DELETE__INIT;
  unsigned char buf[512];
  int len;
  d.streamid = streamid;
  d.starttime = starttime;
  d.endtime = endtime;

  if ((len = delete__get_packed_size(&d)) > sizeof(buf)) {
    goto error;
  }

  delete__pack(&d, buf);
  h.message_type = htonl(MESSAGE_TYPE__DELETE);
  h.body_length = htonl(len);
  /* send it */
  if (fwrite(&h, sizeof(h), 1, ipp->sock_fp) <= 0)
    goto error;
  if (fwrite(buf, len , 1, ipp->sock_fp) <= 0)
    goto error;
  fflush(ipp->sock_fp);
  return;
 error:
  PyErr_SetString(PyExc_IOError, "db_del: generic error");
  return;
}


void db_close(struct sock_request *ipp) {
  fclose(ipp->sock_fp);
  free(ipp);
}
