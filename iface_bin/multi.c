

#include <Python.h>
#include <numpy/arrayobject.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <pthread.h>

#include "readingdb_py.h"

#include "c6/readingdb.h"
#include "c6/rpc.h"
#include "c6/commands.h"

static char host[512];
static short port;
static int workers;
static int substream;
static int setup = 0;

void db_setup(char *a_host, 
              short a_port,
              int a_workers,
              int a_substream) {
  strncpy(host, a_host, sizeof(host));
  port = a_port;
  workers = a_workers;
  substream = a_substream;
  setup = 1;
  import_array();
}

struct np_point {
  double ts;
  double val;
};

struct request {
  pthread_mutex_t lock;
  struct sock_request *conn;

  // server conf
  const char *host;
  const short port;

  // work queue
  int next_streamid;
  int n_streams;

  // request params
  const struct request_desc *r;

  struct np_point **return_data;
  int *return_data_len;
  int errors;
};

int setup_request(struct sock_request *conn,
                  const struct request_desc *req,
                  unsigned long long streamid, 
                  unsigned long long starttime) {
  switch (req->type) {
  case REQ_QUERY:
    return db_query_all(conn, streamid, starttime, req->endtime, QUERY_DATA);
  case REQ_ITER:
    return db_iter(conn, streamid, starttime, req->direction, req->limit);
  default:
    return -1;
  }
}

int read_numpy_resultset(struct sock_request *ipp, 
                         struct np_point **buf, int *off) {
  Response *r;
  struct pbuf_header h;
  void *reply;
  int len, i, rv;

  /* read the reply */
  if (fread(&h, sizeof(h), 1, ipp->sock_fp) <= 0) {
    // PyErr_Format(PyExc_IOError, "read_resultset: error reading from socket: %s", strerror(errno));
    return -errno;
  }
  len = ntohl(h.body_length);
  reply = malloc(len);
  if (!reply) return -1;
  if (fread(reply, len, 1, ipp->sock_fp) <= 0) {
    free(reply);
    // PyErr_Format(PyExc_IOError, "read_resultset: error reading from socket: %s", strerror(errno));
    return -errno;
  }
  r = response__unpack(NULL, len, reply);
  free(reply);
  if (!r) {
    // PyErr_Format(PyExc_IOError, "read_resultset: error unpacking");
    return -errno;
  }
/*   printf("Received reply code: %i results: %li len: %i\n", */
/*          r->error, r->data->n_data, len); */
  if (r->error != RESPONSE__ERROR_CODE__OK) {
    // PyErr_Format(PyExc_Exception, "read_resultset: received error from server: %i", r->error);
    return -errno;
  }

  /* build the python data structure  */
  if (*buf == NULL || *off == 0) {
    len = sizeof(struct np_point) * r->data->n_data;
    *buf = malloc(len);
  } else {
    len = sizeof(struct np_point) * (r->data->n_data + *off);
    *buf = realloc(*buf, len);
    // printf("reallocing\n");
  }
  if (!*buf) {
    fprintf(stderr, "Alloc/realloc failed: request: %i\n", len);
    response__free_unpacked(r, NULL);
    return -1;
  }

  for (i = *off; i < *off + r->data->n_data; i++) {
    ((struct np_point *)(*buf))[i].ts = r->data->data[i - *off]->timestamp;
    ((struct np_point *)(*buf))[i].val = r->data->data[i - *off]->value;
  }
  *off += r->data->n_data;
  rv = r->data->n_data;
  response__free_unpacked(r, NULL);
  
  return rv;
}

void *worker_thread(void *ptr) {
  struct sock_request *conn;
  int id, idx, limit, rv = 0, conn_error = 0;
  unsigned long long starttime;
  struct request *req = ptr;

  // don't need a lock, this might block
  if (req->conn != NULL) {
    conn = req->conn;
  } else {
    conn = __db_open(req->host, req->port, &conn_error);
    if (!conn || conn_error) {
      req->errors = conn_error;
      return NULL;
    }
  }

  while (1) {
    pthread_mutex_lock(&req->lock);
    idx = req->next_streamid;
    if (idx < req->n_streams) {
      id = req->r->streamids[req->next_streamid++];
    } else {
      id = 0;
    }
    pthread_mutex_unlock(&req->lock);
    starttime = req->r->starttime;
    limit = req->r->limit;

    // printf("starting load of %i (%i)\n", id, idx);
    if (id == 0) {
      break;
    }

    // read all the range data from a single stream
    while (1) {
      rv = setup_request(conn, req->r, id, starttime);
      if (rv < 0) {
        fprintf(stderr, "Error from DB: %s\n", strerror(-rv));
        req->errors = -rv;
        goto done;
      }
      rv = read_numpy_resultset(conn, &req->return_data[idx], &req->return_data_len[idx]);
      if (rv < 0) {
        fprintf(stderr, "Error reading results: %s\n", strerror(-rv));
        req->errors = -rv;
        goto done;
      } else if (rv < 10000 || limit - rv <= 0) {
        break;
      }
      limit -= rv;
      starttime = req->return_data[idx][req->return_data_len[idx]-1].ts + 1;
    }
  }
 done:
  if (!req->conn) {
    db_close(conn);
  }
  return NULL;
}

PyObject *make_numpy_list(struct request *req) {
  PyObject *a;
  PyObject *r = PyList_New(req->n_streams);
  int i;
  if (!r) {
    return PyErr_NoMemory();
  }
  for (i = 0; i < req->n_streams; i++) {
    npy_intp dims[2];
    int length = min(req->r->limit, req->return_data_len[i]);

    if (req->return_data && length > 0) {
      dims[0] = length; dims[1] = 2;
      // memcpy into a new array because there's apparently no way to
      // pass off the buffer that will be safe...
      a = PyArray_SimpleNew(2, dims, NPY_DOUBLE);
      if (!a) {
        Py_DECREF(r);
        free(req->return_data[i]);
        return PyErr_NoMemory();
      } else {
        memcpy(PyArray_DATA(a), req->return_data[i], 
               (length * sizeof(struct np_point)));
        free(req->return_data[i]);

        // donate the ref we got
        PyList_SetItem(r, i, a);
      }
    } else {
      npy_intp dims[2] = {0, 2};
      // otherwise return an empty array with the proper dimension.
      // n.b. PyArray_SimpleNew segfaults if any dimensions are zero...
      PyList_SetItem(r, i, PyArray_EMPTY(2, dims, NPY_DOUBLE, 0));
    }
  }
  return r;
}

PyObject *db_multiple(struct sock_request *ipp, const struct request_desc *r) {
    // set up a request;
  PyThreadState *_save;
  PyObject *rv;
  struct request req = {
    .conn = ipp,
    .host = host,
    .port = port,
    .r = r,
    .errors = 0,
  };
  pthread_t threads[workers];
  int n_streams, i;

  if (!setup) {
    PyErr_SetString(PyExc_IOError, 
                    "ERROR: you must call db_setup before using the API\n");
    return NULL;
  }

  _save = PyEval_SaveThread();
  for (n_streams = 0; req.r->streamids[n_streams] != 0; n_streams++);
  // printf("loading %i streams limit %i\n", n_streams, r->limit);

  pthread_mutex_init(&req.lock, NULL);
  req.next_streamid = 0;
  req.n_streams = n_streams;
  req.return_data = malloc(sizeof(PyObject *) * n_streams);
  if (!req.return_data) return NULL;
  req.return_data_len = malloc(sizeof(int) * n_streams);
  if (!req.return_data) {
    free(req.return_data);
    PyEval_RestoreThread(_save);
    return PyErr_NoMemory();
  }
  memset(req.return_data_len, 0, sizeof(int) * n_streams);
  for (i = 0; i < n_streams; i++) req.return_data[i] = NULL;

  if (n_streams == 1 || ipp) {
    // printf("not starting threads because connection is provided: %p\n", ipp);
    worker_thread(&req);
  } else {
    int my_workers = min(n_streams, workers);
    // printf("starting %i workers\n", my_workers);
    for (i = 0; i < my_workers; i++) {
      pthread_create(&threads[i], NULL, worker_thread, &req);
    }
    for (i = 0; i < my_workers; i++) {
      void *code;
      pthread_join(threads[i], &code);
    }
  }

  PyEval_RestoreThread(_save);
  // printf("req errors %i\n", req.errors);
  if (!req.errors) {
    rv = make_numpy_list(&req);
    free(req.return_data);
    free(req.return_data_len);
    return rv;
  } else {
    for (i = 0; i < n_streams; i++) {
      if (req.return_data[i])
        free(req.return_data[i]);
    }
    free(req.return_data);
    free(req.return_data_len);
    PyErr_Format(PyExc_Exception, "error reading data: last error: %s", 
                 strerror(req.errors));
    return NULL;
  }
}
