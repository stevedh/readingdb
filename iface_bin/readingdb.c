
#include <Python.h>
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

#include "../c/readingdb.h"
#include "../c/rpc.h"
// #include "../c/pbuf/rdb.pb-c.h"

#define TIMEOUT_SECS 10
#define SIGREPLACE SIGTERM

struct sock_request *db_open(char *host, short port) {
  struct sock_request *req = malloc(sizeof(struct sock_request));
  struct addrinfo *res, hints;
  struct sockaddr_in dest;
  struct timeval timeout;

  if (!req) 
    return (struct sock_request *)PyErr_NoMemory();

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  if (getaddrinfo(host, NULL, &hints, &res) != 0) {
    PyErr_Format(PyExc_IOError, "could not resolve host: %s", host);
    return NULL;
  }

  memcpy(&dest, res->ai_addr, res->ai_addrlen);
  dest.sin_port = htons(port);
  freeaddrinfo(res);

  req->sock = socket(AF_INET, SOCK_STREAM, 0);
  if (req->sock < 0) {
    PyErr_Format(PyExc_IOError, "socket: %s", strerror(errno));
    goto cleanup;
  }

  timeout.tv_sec = 30;
  timeout.tv_usec = 0;
  if (setsockopt(req->sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
    PyErr_Format(PyExc_IOError, "setsockopt: %s", strerror(errno));
    goto cleanup;
  }

  if (connect(req->sock, &dest, sizeof(dest)) < 0) {
    PyErr_Format(PyExc_IOError, "connect: %s", strerror(errno));
    goto cleanup;
  }

  req->sock_fp = fdopen(req->sock, "r+");
  if (req->sock_fp == NULL) {
    PyErr_Format(PyExc_IOError, "fdopen: %s", strerror(errno));
    close(req->sock);
    goto cleanup;
  }
  
  req->substream = 0;
  return req;
 cleanup:
  free(req);
  if (!PyErr_Occurred())
    PyErr_SetString(PyExc_IOError, "Generic error");
  return NULL;
}

void db_substream(struct sock_request *ipp, int substream) {
  if (ipp)
    ipp->substream = substream;
}

int db_add(struct sock_request *ipp, int streamid, PyObject *values) {
  int i, len;
  ReadingSet *r;
  struct pbuf_header h;
  unsigned char *buf;

  r = _rpc_alloc_rs(SMALL_POINTS);
  if (!r) {
    PyErr_SetNone(PyExc_MemoryError);
    return -1;
  }

  r->streamid = streamid;
  r->substream = ipp->substream;

  if (!PyList_Check(values)) {
    _rpc_free_rs(r);
    return -1;
  }

  if (PyList_Size(values) > SMALL_POINTS) {
    _rpc_free_rs(r);
    return -1;
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
    return -1;
  }
  reading_set__pack(r, buf);
  _rpc_free_rs(r);

  h.body_length = htonl(len);
  h.message_type = htonl(MESSAGE_TYPE__READINGSET);
  
  if (fwrite(&h, sizeof(h), 1, ipp->sock_fp) <= 0) {
    free(buf);
    PyErr_Format(PyExc_IOError, "error writing to socket: %s", strerror(errno));
    return -1;
  }
  if (fwrite(buf, len, 1, ipp->sock_fp) <= 0) {
    free(buf);
    PyErr_Format(PyExc_IOError, "error writing to socket: %s", strerror(errno));
    return -1;
  }
  free(buf);
  return 1;
}

PyObject * read_resultset(struct sock_request *ipp) {
  PyObject *ret, *record;
  Response *r;
  struct pbuf_header h;
  void *reply;
  int len, i;

  /* read the reply */
  if (fread(&h, sizeof(h), 1, ipp->sock_fp) <= 0) {
    PyErr_Format(PyExc_IOError, "read_resultset: error reading from socket: %s", strerror(errno));
    return NULL;
  }
  len = ntohl(h.body_length);
  reply = malloc(len);
  if (!reply) return NULL;;
  if (fread(reply, len, 1, ipp->sock_fp) <= 0) {
    free(reply);
    PyErr_Format(PyExc_IOError, "read_resultset: error reading from socket: %s", strerror(errno));
    return NULL;
  }
  r = response__unpack(NULL, len, reply);
  free(reply);
  if (!r) {
    PyErr_Format(PyExc_IOError, "read_resultset: error unpacking");
    return NULL;
  }
/*   printf("Received reply code: %i results: %li len: %i\n", */
/*          r->error, r->data->n_data, len); */

  /* build the python data structure  */
  ret = PyList_New(r->data->n_data);
  if (ret == NULL) {
    response__free_unpacked(r, NULL);
    /* PyList_New sets exception */
    return NULL;
  }

  for (i = 0; i < r->data->n_data; i++) {
    if (r->data->data[i]->has_min && r->data->data[i]->has_max) {
      record = Py_BuildValue("llddd", r->data->data[i]->timestamp,
                             r->data->data[i]->has_seqno ? r->data->data[i]->seqno : 0,                               
                             r->data->data[i]->value,
                             r->data->data[i]->min,
                             r->data->data[i]->max);
    } else {
      record = Py_BuildValue("lld", r->data->data[i]->timestamp,
                             r->data->data[i]->has_seqno ? r->data->data[i]->seqno : 0,                               
                             r->data->data[i]->value);
    }
    PyList_SetItem(ret, i, record);
  }
  response__free_unpacked(r, NULL);
  return ret;
}

PyObject *db_query(struct sock_request *ipp, unsigned long long streamid, 
                   unsigned long long starttime, 
                   unsigned long long endtime) {
  Query q = QUERY__INIT; 
  struct pbuf_header h;
  unsigned char buf [512];
  int len;

  q.streamid = streamid;
  q.substream = ipp->substream;
  q.starttime = starttime;
  q.endtime = endtime;

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

    return read_resultset(ipp);
  }
 write_error:
  PyErr_Format(PyExc_IOError, "db_query: error writing: %s", strerror(errno));
  return NULL;
}

PyObject *db_iter(struct sock_request *ipp, int streamid, 
                  unsigned long long reference, int direction) {
  struct pbuf_header h;
  Nearest n = NEAREST__INIT;
  unsigned char buf[512];
  int len;

  n.streamid = streamid;
  n.substream = ipp->substream;
  n.reference = reference;
  n.direction = direction;

  if ((len = nearest__get_packed_size(&n)) > sizeof(buf)) {
    PyErr_SetString(PyExc_IOError, "db_next: generic error");
    return NULL;
  }

  h.message_type = htonl(MESSAGE_TYPE__NEAREST);
  h.body_length = htonl(len);
  nearest__pack(&n, buf);
  
  if (fwrite(&h, sizeof(h), 1, ipp->sock_fp) <= 0)
    goto write_error;
  if (fwrite(buf, len, 1, ipp->sock_fp) <= 0)
    goto write_error;
  fflush(ipp->sock_fp);

  return read_resultset(ipp);

 write_error:
  PyErr_Format(PyExc_IOError, "db_iter: error writing: %s", strerror(errno));
  return NULL;
}

PyObject *db_next(struct sock_request *ipp, int streamid, 
                  unsigned long long reference) {
  return db_iter(ipp, streamid, reference, NEAREST__DIRECTION__NEXT);
}

PyObject *db_prev(struct sock_request *ipp, int streamid, 
                  unsigned long long reference) {
  return db_iter(ipp, streamid, reference, NEAREST__DIRECTION__PREV);
}

void db_close(struct sock_request *ipp) {
  fclose(ipp->sock_fp);
  free(ipp);
}
