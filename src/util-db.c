
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <semaphore.h>
#include <string.h>
#include <signal.h>
#include <pwd.h>
#include <string.h>
#include <errno.h>
#include <db.h>
#include <arpa/inet.h>
#include <assert.h>

#include "logging.h"
#include "readingdb.h"
#include "config.h"

#ifdef WRITE_COMPRESSION_LOG
#define CMPR_LOG "cmpr.log"
static void write_cmprlog(unsigned long long streamid,
                          unsigned long long substream,
                          unsigned long long base,
                          int nvalid,
                          size_t uncmpr, size_t cmpr) {
  FILE *fp = fopen(CMPR_LOG, "a");
  if (!fp) return;
  fprintf(fp, "%llu,%llu,%llu,%i,%llu,%llu\n", 
          streamid, substream, base, nvalid, uncmpr, cmpr);
  fclose(fp);
}
#endif

int put(DB *dbp, DB_TXN *txn, struct rec_key *k, struct rec_val *v) {
  struct rec_key put_k;
  int ret;
  DBT key, data;
  int len;
  void *data_buf;

#ifdef USE_COMPRESSION
  char cmpr_buf[COMPRESS_WORKING_BUF];
  if ((ret = val_compress(v, cmpr_buf, sizeof(cmpr_buf))) < 0) {
    warn("Compression failed!\n");
    assert (0);
  }

  debug("compress: streamid: %i timestamp: 0x%x n_valid: %i %lu -> %i\n", 
        k->stream_id, k->timestamp, 
        v->n_valid, POINT_OFF(v->n_valid), ret);

#ifdef WRITE_COMPRESSION_LOG
  write_cmprlog(k->stream_id, 0, k->timestamp, 
                v->n_valid, POINT_OFF(v->n_valid), ret);
#endif

  data_buf = cmpr_buf;
  len = ret;
#else
#warn Database compression disabled
  data_buf = v;
  len = POINT_OFF(v->n_valid);
#endif
 
  bzero(&key, sizeof(key));
  bzero(&data, sizeof(data));

  debug("put stream_id: %i timestamp: 0x%x len: %i\n",
        k->stream_id, k->timestamp, len);

  put_k.stream_id = htonl(k->stream_id);
  put_k.timestamp = htonl(k->timestamp);

  key.data = &put_k;
  key.size = key.ulen = sizeof(struct rec_key);
  key.flags = DB_DBT_USERMEM;

  data.data = data_buf;
  data.size = data.ulen = len;
  data.flags = DB_DBT_USERMEM;

  if ((ret = dbp->put(dbp, txn, &key, &data, 0)) != 0) {
    error("db put: %s\n", db_strerror(ret));
    return -1;
  }
  return 0;
}

/** Lookup the key passed in using an exact match
 * @cursorp cursor to use
 * @k (input) the key to lookup
 * @buf output buffer of length
 * @len
 */
int get(DBC *cursorp, int flags, struct rec_key *k, struct rec_val *v, int len) {
  int ret;
  struct rec_key get_k;
  DBT key, data;
  char zbuf[COMPRESS_WORKING_BUF];
  bzero(&key, sizeof(key));
  bzero(&data, sizeof(data));

  debug("get stream_id: %i timestamp: 0x%x\n", k->stream_id, k->timestamp);

  get_k.stream_id = htonl(k->stream_id);
  get_k.timestamp = htonl(k->timestamp);

  key.data = &get_k;
  key.size = key.ulen = sizeof(struct rec_key);
  key.flags = DB_DBT_USERMEM;
  
#ifdef USE_COMPRESSION
  data.data = zbuf;
  data.size = data.ulen = sizeof(zbuf);
  data.flags = DB_DBT_USERMEM;
#else
  data.data = v;
  data.size = data.ulen = len;
  data.flags = DB_DBT_USERMEM;
#endif
  
  if ((ret = cursorp->get(cursorp, &key, &data, flags)) == 0) {
    if (flags & DB_NEXT || flags & DB_SET_RANGE) {
      k->stream_id = ntohl(get_k.stream_id);
      k->timestamp = ntohl(get_k.timestamp);
    }

#ifdef USE_COMPRESSION
    val_decompress(zbuf, data.size, v, len);

    debug("decompress: streamid: %i timestamp: 0x%x n_valid: %i, %u -> %u\n", 
          k->stream_id, k->timestamp, v->n_valid,
          data.size, POINT_OFF(v->n_valid));

#endif
    return 0;
  }
  if (ret != DB_NOTFOUND)
    warn("Get failed: %s\n", db_strerror(ret));
  return ret;
}

int get_partial(DBC *cursorp, int flags, struct rec_key *k, 
                void *buf, int len, int off) {
  char unpacked[COMPRESS_WORKING_BUF];
  int ret;

  debug("get_partial stream_id: %i timestamp: 0x%x\n",
        k->stream_id, k->timestamp);

  if ((ret = get(cursorp, flags, k, (struct rec_val *)unpacked, sizeof(unpacked))) < 0) {
    return ret;
  }

  memcpy(buf, unpacked + off, len);

  return ret;
}
