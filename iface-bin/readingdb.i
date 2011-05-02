
%module readingdb
%{
#include "readingdb_py.h"
%}

%exception db_open {
  $action
  if (!result) {
    return NULL;
  }
}
struct sock_request *db_open(char *host="jackalope.cs.berkeley.edu", short port=4243);


void db_substream(struct sock_request *dbp, int substream);
void db_close(struct sock_request *dbp);

%exception db_query {
  $action
  if (!result)
    return NULL;
}
PyObject *db_query(struct sock_request *dbp, unsigned long long streamid, 
        unsigned long long starttime, 
        unsigned long long endtime);

%exception db_next {
  $action
  if (!result) return NULL;
}
PyObject * db_next(struct sock_request *dbp, int streamid, unsigned long long reference);

%exception db_prev {
  $action
  if (!result) return NULL;
}
PyObject * db_prev(struct sock_request *dbp, int streamid, unsigned long long reference);

%exception db_add {
  $action
  if (!result) return NULL;
}
int db_add(struct sock_request *dbp, int streamid, PyObject *data);
