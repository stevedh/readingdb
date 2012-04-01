
%module readingdb
%{
#include "readingdb_py.h"
%}

// convert a python list of ints to a array of long longs.
%typemap(in) unsigned long long * {
  if (PyList_Check($input)) {
    int size = PyList_Size($input);
    int i = 0;
    $1 = (unsigned long long *)malloc(sizeof(unsigned long long) * (size + 1));
    for (i = 0; i < size; i++) {
      PyObject *o = PyList_GetItem ($input, i);
      if (PyInt_Check(o)) {
        $1[i] = PyInt_AsUnsignedLongMask(o);
      } else {
        PyErr_SetString(PyExc_TypeError, "Stream IDs must all be Integers");
        free($1);
        return NULL;
      }
    }
    $1[i] = 0;
  } else {
    PyErr_SetString(PyExc_TypeError, "Not a list of streamids!");
    return NULL;
  }
}
%typemap(freearg) unsigned long long * {
  free($1);
}

%exception db_open {
  $action
  if (!result) return NULL;        
}
struct sock_request *db_open(char *host="gecko.cs.berkeley.edu", short port=4242);


void db_substream(struct sock_request *dbp, int substream);
void db_close(struct sock_request *dbp);

%exception db_query {
  $action
  if (!result) return NULL;
}
PyObject *db_query(struct sock_request *dbp, unsigned long long streamid, 
        unsigned long long starttime, 
        unsigned long long endtime);


%exception db_multiple {
  $action
  if (!result) return NULL;
}
PyObject *db_multiple(unsigned long long *streamids,
                      unsigned long long starttime, 
                      unsigned long long endtime);


void db_setup(char *a_host, 
              short a_port,
              int a_workers=5,
              int a_substream=0);

%exception db_count {
  $action
  if (!result) return NULL;
}
PyObject *db_count(struct sock_request *dbp, unsigned long long streamid, 
        unsigned long long starttime, 
        unsigned long long endtime);

%exception db_next {
  $action
  if (!result) return NULL;
}
PyObject * db_next(struct sock_request *dbp, int streamid, unsigned long long reference, int n = 1);

%exception db_prev {
  $action
  if (!result) return NULL;
}
PyObject * db_prev(struct sock_request *dbp, int streamid, unsigned long long reference, int n = 1);

%exception db_add {
  $action
  if (!result) return NULL;
}
int db_add(struct sock_request *dbp, int streamid, PyObject *data);

void db_del(struct sock_request *dbp, int streamid, 
            unsigned long long starttime,
            unsigned long long endtime);
