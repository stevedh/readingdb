#ifndef _READINGDB_PY_H_
#define _READINGDB_PY_H_

#include <Python.h>
#include <stdint.h>

#include "c6/readingdb.h"
#include "c6/rpc.h"
#include "c6/commands.h"

struct request_desc {
  enum {
    REQ_QUERY, REQ_ITER,
  } type;
  unsigned long long *streamids;
  unsigned long long starttime; 
  unsigned long long endtime;
  int direction;
  int limit;
};

struct pyerr {
  PyObject *exception;
  char message[512];
};
  
/* exported python definition */
struct sock_request *db_open(const char *host, const short port);
struct sock_request *__db_open(const char *host, const short port, int *error);
void db_close(struct sock_request *dbp);
void db_setup(char *a_host, 
              short a_port,
              int a_workers,
              int a_substream);

/* queries */
PyObject *db_query(unsigned long long *streamids, 
                   unsigned long long starttime, 
                   unsigned long long endtime,
                   int limit,
                   struct sock_request *ipp) ;
PyObject *db_next(unsigned long long *streamid, 
                  unsigned long long reference, 
                  int n,
                  struct sock_request *ipp);
PyObject *db_prev(unsigned long long *streamid, 
                  unsigned long long reference, 
                  int n,
                  struct sock_request *ipp);

/* modifications */
int db_add(struct sock_request *dbp, int streamid, PyObject *values);
void db_del(struct sock_request *ipp, 
            unsigned long long streamid, 
            unsigned long long starttime, 
            unsigned long long endtime);



/* non-api functions */

/* request-generating functions -- write a request out to the socket */
int db_query_all(struct sock_request *ipp, unsigned long long streamid, 
                 unsigned long long starttime, 
                 unsigned long long endtime,
                 enum query_action action);
int db_iter(struct sock_request *ipp, int streamid, 
                  unsigned long long reference, int direction, int ret_n);

PyObject *db_multiple(struct sock_request *ipp, const struct request_desc *req);

#endif
