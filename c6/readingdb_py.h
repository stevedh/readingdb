#ifndef _READINGDB_PY_H_
#define _READINGDB_PY_H_

#include <Python.h>
#include <stdint.h>
#include <db.h>

#include "readingdb.h"

/* exported python definition */
IPC *db_open(void);
void db_substream(IPC *dpb, int substream);
void db_close(IPC *dbp);
int db_sync(IPC *dbp);
PyObject *db_query(IPC *dbp, unsigned long long streamid, 
                   unsigned long long starttime, 
                   unsigned long long endtime) ;
int db_add(IPC *dbp, int streamid, PyObject *values);

#endif
