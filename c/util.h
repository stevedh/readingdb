#ifndef _UTIL_H
#define _UTIL_H

/* IPC *ipc_open(int semflags, int memflags); */
/* void ipc_close(IPC *ipp); */
void drop_priv(void);

int timeval_subtract (struct timeval *result, struct timeval *x, struct timeval *y);

int put(DB *dbp, DB_TXN *txn, struct rec_key *k, void *buf, int len);
int put_partial(DB *dbp, DB_TXN *txn, struct rec_key *k, void *buf, int len, int off);
int get_partial(DBC *cursorp, int flags, struct rec_key *k, void *buf, int len, int off);
int get(DBC *cursorp, int flags, struct rec_key *k, void *buf, int len);

#define FREELIST(type, name) ;
/* #define FREELIST(type, name) \ */
/*  static type *freelist_ ## name = NULL; \ */
/*  static type *freelist_ ## name ## _tmp; */

#define FREELIST_GET(type, name) ((type *)malloc(sizeof(type)))
/* #define FREELIST_GET(type, name) \ */
/*  ((freelist_ ## name) == NULL ? ((type *)(malloc(sizeof(type)))) : \ */
/*  ((freelist_ ## name ## _tmp) = freelist_ ## name, \ */
/*   freelist_ ## name = *((type **)(freelist_ ## name)), \ */
/*   freelist_ ## name ## _tmp)) */

#define FREELIST_PUT(type, name, val) free(val)

/* #define FREELIST_PUT(type, name, val)  {\ */
/*    *(type **)(val) = freelist_ ## name; \ */
/*    freelist_ ## name = val;             \ */
/* } */
#endif
