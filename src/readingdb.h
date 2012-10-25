#ifndef _READINGDB_H_
#define _READINGDB_H_

#include <stdint.h>
#include <semaphore.h>
#include <stdio.h>
#include <db.h>

/* types used for shared memory communication */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define MAX_SUBSTREAMS 10

/* max number of client threads */
#define MAXCONCURRENCY 150

#define NBUCKETSIZES 3
#define MAXBUCKETRECS (60 * 5)  /* has to be >= the number of records
                                                  in the smallest bucket */


#define COMPRESS_WORKING_BUF 64000


#define DEFAULT_PAGESIZE 16384
/* must be sorted */
extern int bucket_sizes[NBUCKETSIZES];

#define MAXQUERYSET 128

/* record definitions for the bdb instance */
struct rec_key {
  /* these are in network byte-order */
  uint32_t stream_id;
  uint32_t timestamp;
};

struct point {
  uint32_t timestamp;
  uint32_t reading_sequence;
  double reading;
  double min;
  double max;
} __attribute__((packed));

#define POINT_OFF(IDX) ((((IDX) > 0 ? (IDX) : 0) * (sizeof(struct point))) + (sizeof (struct rec_val)))
struct rec_val {
  uint32_t n_valid;
  uint32_t period_length;
  uint32_t tail_timestamp;
  struct point data[0];
};

#define SMALL_POINTS 128
struct point_list {
  int n;
  struct point v[SMALL_POINTS];
};

struct sock_request {
  int sock;
  FILE *sock_fp;
  int substream;
};

struct ipc_command {
  enum {
    COMMAND_QUERY = 1,
    COMMAND_ADD = 2,
    COMMAND_SYNC = 3,
  } command;
  int dbid;
  unsigned long long streamid;
  union {
    struct {
      unsigned long long starttime;
      unsigned long long endtime;
    } query;
    struct point_list add;
  } args;
};

struct ipc_reply {
  enum {
    REPLY_OK = 0,
    REPLY_QUERY = 1,
    REPLY_ERROR = 2,
  } reply;
  union {
    struct {
      int nrecs;
      struct point pts[0];
    } query;
    enum {
      E_INVAL_SUBSTREAM = 1,
    } error;
  } data;;
};

struct pbuf_header {
  uint32_t message_type;
  uint32_t body_length;
};

#define MAX_PBUF_MESSAGE 1000000

/* compression functions in compress.c */
int val_compress(struct rec_val *v, void *buf, int len);
int val_decompress(void *cmpr, int cmpr_len, struct rec_val *v, int v_len);

typedef enum {
  FALSE = 0, TRUE = 1
} bool_t;

#endif
