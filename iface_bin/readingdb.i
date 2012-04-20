
%module readingdb
%{
#include "readingdb_py.h"
%}

%exception db_open {
  $action
  if (!result) return NULL;        
}
struct sock_request *db_open(char *host="localhost", short port=4242);
void db_close(struct sock_request *dbp);


// set up with the right database
void db_setup(char *a_host, 
              short a_port,
              int a_workers=5,
              int a_substream=0);

// convert a python list of ints to a array of long longs.
// if it's just one int, make that work too
%typemap(in) unsigned long long * {
  if (PyList_Check($input)) {
    int size = PyList_Size($input);
    int i = 0;
    $1 = (unsigned long long *)malloc(sizeof(unsigned long long) * (size + 1));
    if (!$1) return NULL;
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
  } else if (PyInt_Check($input)) {
    $1 = (unsigned long long *)malloc(sizeof(unsigned long long) * 2);
    if (!$1) return NULL;
    $1[0] = PyInt_AsLong($input);
    $1[1] = 0;
  } else {
    PyErr_SetString(PyExc_TypeError, "Not a list of streamids!");
    return NULL;
  }
}
%typemap(freearg) unsigned long long * {
  free($1);
}

%exception db_query {
  $action
    if (!result) return NULL;
}
PyObject *db_query(unsigned long long *streamid, 
                   unsigned long long starttime, 
                   unsigned long long endtime,
                   int limit = 10000,
                   struct sock_request *conn = NULL);

%exception db_next {
  $action
  if (!result) return NULL;
}
PyObject * db_next(unsigned long long *streamids, 
                   unsigned long long reference, 
                   int n = 1,
                   struct sock_request *conn = NULL);

%exception db_prev {
  $action
  if (!result) return NULL;
}
PyObject * db_prev(unsigned long long *streamids, 
                   unsigned long long reference, 
                   int n = 1,
                   struct sock_request *conn = NULL);

%exception db_add {
  $action
  if (!result) return NULL;
}
int db_add(struct sock_request *dbp, int streamid, PyObject *data);

void db_del(struct sock_request *dbp, int streamid, 
            unsigned long long starttime,
            unsigned long long endtime);
