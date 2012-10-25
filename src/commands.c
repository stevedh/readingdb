
#include <time.h>
#include <db.h>
#include <signal.h>
#include <string.h>

#include "readingdb.h"
#include "util.h"
#include "logging.h"
#include "pbuf/rdb.pb-c.h"
#include "commands.h"


extern DB_ENV *env;
extern sig_atomic_t do_shutdown;

void query(DB *dbp, Query *q, Response *r, enum query_action action) {
  int ret;
  DBC *cursorp;
  struct rec_key k;
  struct rec_val v;
  unsigned long long starttime, endtime;
  int streamid;
  int cursor_flags = 0;
  
  streamid = q->streamid;
  starttime = q->starttime;
  endtime = q->endtime;
  debug("starting query id: %i start: %i end: %i\n", streamid, starttime, endtime);

  /* set up the query key */
  k.stream_id = streamid;
  k.timestamp = starttime - (starttime % bucket_sizes[NBUCKETSIZES-1]);

  switch (action) {
  case QUERY_COUNT:
    /* initialize our return with one guy in it */
    reading__init(r->data->data[0]);
    reading__init(r->data->data[1]);
    r->data->data[0]->timestamp = 
      r->data->data[1]->timestamp = 
      time(NULL);
    r->data->n_data = 2;
    break;
  case QUERY_DATA:
    break;
  default:
    /* invalid request type */
    r->error = RESPONSE__ERROR_CODE__FAIL_PARAM;
    return;
  }

  ret = dbp->cursor(dbp, NULL, &cursorp, cursor_flags);
  if (cursorp == NULL) {
    dbp->err(dbp, ret, "cursor");
    r->error = RESPONSE__ERROR_CODE__FAIL;
    return;
  }

  if (get_partial(cursorp, DB_SET_RANGE, &k, &v, sizeof(struct rec_val), 0) != 0) {
    goto done;
  }

  do {
    int i;
    int read_recs = min(v.n_valid, MAXRECS - r->data->n_data);
    struct point bucket[MAXBUCKETRECS + NBUCKETSIZES];
    debug("examining record start: 0x%x length: %i streamid: %i\n", 
          k.timestamp, v.period_length, k.stream_id);
    if (streamid != k.stream_id) break;
    if (k.timestamp >= endtime) break;
    if (r->data->n_data >= MAXRECS) break;
    if (!valid_bucketsize(v.period_length) || v.n_valid > MAXBUCKETRECS + NBUCKETSIZES) {
      warn("length is invalid: %i! streamid: %i start: %i nvalid: %i\n", 
	   v.period_length, k.stream_id, k.timestamp, v.n_valid);
      goto next;
    }

    switch (action) {
    case QUERY_DATA:
      if (get_partial(cursorp, DB_SET, &k, bucket,
                      sizeof(struct point) * read_recs,
                      sizeof(struct rec_val)) < 0) {
        goto next;
      }
      for (i = 0; i < read_recs; i++) {
        if (bucket[i].timestamp >= starttime &&
            bucket[i].timestamp < endtime) {
          _rpc_copy_reading(r->data->data[r->data->n_data++], &bucket[i]);
        }
      }
      debug("query: added %i/%i records\n", r->data->n_data, v.n_valid);
      break;
    case QUERY_COUNT:
      r->data->data[0]->value += v.n_valid;
      r->data->data[1]->value += 1;
      break;
    }
  next:
    ;
  } while (get_partial(cursorp, DB_NEXT, &k, &v, 
                       sizeof(struct rec_val), 0) == 0);

  debug("returning %i records\n", r->data->n_data);
  r->error = RESPONSE__ERROR_CODE__OK;
  
 done:
  cursorp->close(cursorp);
}

void query_nearest(DB *dbp, Nearest *n, Response *rs, enum query_action action) {
  int ret;
  DBC *cursorp;
  struct rec_key k;
  struct rec_val v;
  unsigned long long starttime;
  int streamid, i, rc;
  int direction = n->direction == NEAREST__DIRECTION__NEXT ? DB_NEXT : DB_PREV;
  int ret_n = n->has_n ? min(MAXRECS, n->n) : 1;

  streamid = n->streamid;
  starttime = n->reference;
  debug("starting nearest query id: %i reference: %i\n", streamid, starttime);

  /* set up the query key */
  k.stream_id = streamid;
  if (direction == DB_NEXT) {
    k.timestamp = starttime - (starttime % bucket_sizes[NBUCKETSIZES-1]);
  } else if (direction == DB_PREV) {
    k.timestamp = starttime - (starttime % bucket_sizes[0]);
  }

  rs->error = RESPONSE__ERROR_CODE__OK;
  rs->data->n_data = 0;

  ret = dbp->cursor(dbp, NULL, &cursorp, 0);
  if (cursorp == NULL) {
    dbp->err(dbp, ret, "cursor");
    return;
  }

  if ((rc = get_partial(cursorp, DB_SET_RANGE, &k, &v, sizeof(struct rec_val), 0)) < 0) {
    // this might put us at the end of the database...
    if (direction == DB_PREV && rc == DB_NOTFOUND) goto next;
    else goto done;
  }

  do {
    struct point bucket[MAXBUCKETRECS + NBUCKETSIZES];
    debug("examining record start: %i length: %i streamid: %i\n", 
          k.timestamp, v.period_length, k.stream_id);
    if ((direction == DB_NEXT && k.stream_id > streamid) ||
        (direction == DB_PREV && k.stream_id < streamid)) break;
    if ((direction == DB_NEXT && k.timestamp + v.period_length < starttime) ||
        (direction == DB_PREV && k.timestamp > starttime)) goto next;
    if (get_partial(cursorp, DB_SET, &k, bucket,
                    sizeof(struct point) * v.n_valid,
		    sizeof(struct rec_val)) < 0) {
     goto next;
    }

    for (i = (direction == DB_NEXT ? 0 : v.n_valid - 1);
         (direction == DB_NEXT ? i < v.n_valid : i >= 0);
         (direction == DB_NEXT ? i++ : i--)) {
      if (k.stream_id == streamid && 
          ((direction == DB_NEXT && bucket[i].timestamp > starttime) ||
           (direction == DB_PREV && bucket[i].timestamp < starttime))) {
        /* return */
        _rpc_copy_reading(rs->data->data[rs->data->n_data++], &bucket[i]);
        if (rs->data->n_data >= ret_n)
          goto done;
      } 
    }

  next:
    ;
  } while (get_partial(cursorp, direction, &k, &v,
                       sizeof(struct rec_val), 0) == 0);
 done:
  cursorp->close(cursorp);
}

int del(DB *dbp, Delete *d) {
  int ret;
  DBC *cursorp;
  DB_TXN *tid = NULL;
  struct rec_key k;
  struct rec_val v;
  unsigned long long starttime, endtime;
  int streamid;
  
  streamid = d->streamid;
  starttime = d->starttime;
  endtime = d->endtime;
  debug("starting query id: %i start: %i end: %i\n", streamid, starttime, endtime);

  /* set up the query key */
  k.stream_id = streamid;
  k.timestamp = starttime - (starttime % bucket_sizes[NBUCKETSIZES-1]);

  if ((ret = env->txn_begin(env, NULL, &tid, 0)) != 0) {
    error("txn_begin: %s\n", db_strerror(ret));
    return -1;
  }

  ret = dbp->cursor(dbp, tid, &cursorp, 0);
  if (cursorp == NULL) {
    dbp->err(dbp, ret, "cursor");
    goto abort;
  }


  if (get_partial(cursorp, DB_SET_RANGE, &k, &v, sizeof(struct rec_val), 0) != 0) {
    goto abort;
  }

  do {
    debug("delete: examining record start: 0x%x length: %i streamid: %i=%i\n", 
          k.timestamp, v.period_length, k.stream_id, streamid);
    if (k.stream_id < streamid) goto next;
    else if (k.stream_id > streamid) break;
    if (k.timestamp < starttime) goto next;;
    if (k.timestamp + v.period_length > endtime) break;
    if (!valid_bucketsize(v.period_length) || v.n_valid > MAXBUCKETRECS + NBUCKETSIZES) {
      warn("delete: length is invalid: %i! streamid: %i start: %i nvalid: %i\n", 
	   v.period_length, k.stream_id, k.timestamp, v.n_valid);
    }

    if (cursorp->c_del(cursorp, 0) != 0) {
      goto abort;
    }
    debug("delete: removed %i records\n", v.n_valid);
  next:
    debug("checking next\n");
  } while (get_partial(cursorp, DB_NEXT, &k, &v, 
                       sizeof(struct rec_val), 0) == 0);
  
  cursorp->close(cursorp);

  if ((ret = tid->commit(tid, 0)) != 0) {
    fatal("transaction commit failed: %s\n", db_strerror(ret));
    do_shutdown = 1;
  }

  return 0;

 abort:
  cursorp->close(cursorp);
  warn("delete: Aborting transaction\n");

  if ((ret = tid->abort(tid)) != 0) {
    fatal("delete: Could not abort transaction: %s\n", db_strerror(ret));
    // do_shutdown = 1;
    assert(0);
  }
  return -1;

}
