#ifndef _READINGDB_PY_H_
#define _READINGDB_PY_H_

#include <Python.h>
#include <stdint.h>

#include "../c6/readingdb.h"
#include "../c6/rpc.h"
#include "../c6/commands.h"

/* exported python definition */
struct sock_request *db_open(const char *host, const short port);
void db_substream(struct sock_request *dpb, int substream);
void db_close(struct sock_request *dbp);
PyObject *db_query(struct sock_request *dbp, unsigned long long streamid, 
                   unsigned long long starttime, 
                   unsigned long long endtime) ;
PyObject *db_count(struct sock_request *dbp, unsigned long long streamid, 
                   unsigned long long starttime, 
                   unsigned long long endtime) ;
PyObject *db_next(struct sock_request *dbp, int streamid, unsigned long long reference, int n);
PyObject *db_prev(struct sock_request *dbp, int streamid, unsigned long long reference, int n);
int db_add(struct sock_request *dbp, int streamid, PyObject *values);
void db_del(struct sock_request *ipp, 
            unsigned long long streamid, 
            unsigned long long starttime, 
            unsigned long long endtime);
int db_query_all(struct sock_request *ipp, unsigned long long streamid, 
                 unsigned long long starttime, 
                 unsigned long long endtime,
                 enum query_action action);


void db_setup(char *a_host, 
              short a_port,
              int a_workers,
              int a_substream);
PyObject *db_multiple(unsigned long long *streamids,
                      unsigned long long starttime, 
                      unsigned long long endtime);

#endif
