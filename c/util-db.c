
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

#include "logging.h"
#include "readingdb.h"

int put(DB *dbp, DB_TXN *txn, struct rec_key *k, void *buf, int len) {
  struct rec_key put_k;
  DBT key, data;
  bzero(&key, sizeof(key));
  bzero(&data, sizeof(data));

  debug("put stream_id: %i timestamp: 0x%x len: %i\n",
        k->stream_id, k->timestamp, len);

  put_k.stream_id = htonl(k->stream_id);
  put_k.timestamp = htonl(k->timestamp);

  key.data = &put_k;
  key.size = key.ulen = sizeof(struct rec_key);
  key.flags = DB_DBT_USERMEM;

  data.data = buf;
  data.size = data.ulen = len;
  data.flags = DB_DBT_USERMEM;

  if (dbp->put(dbp, txn, &key, &data, 0) != 0) {
    return -1;
  }
  return 0;
}

int put_partial(DB *dbp, DB_TXN *txn, struct rec_key *k, void *buf, int len, int off) {
  struct rec_key put_k;
  DBT key, data;
  bzero(&key, sizeof(key));
  bzero(&data, sizeof(data));

  debug("put_partial stream_id: %i timestamp: 0x%x len: %i offset: %i\n",
        k->stream_id, k->timestamp, len, off);

  put_k.stream_id = htonl(k->stream_id);
  put_k.timestamp = htonl(k->timestamp);

  key.data = &put_k;
  key.size = key.ulen = sizeof(struct rec_key);
  key.flags = DB_DBT_USERMEM;

  data.data = buf;
  data.size = data.ulen = data.dlen = len;
  data.doff = off;
  data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

  if (dbp->put(dbp, txn, &key, &data, 0) != 0) {
    warn("put partial failed!\n");
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
int get(DBC *cursorp, int flags, struct rec_key *k, void *buf, int len) {
  int ret;
  struct rec_key get_k;
  DBT key, data;
  bzero(&key, sizeof(key));
  bzero(&data, sizeof(data));

  debug("get stream_id: %i timestamp: 0x%x\n", k->stream_id, k->timestamp);

  get_k.stream_id = htonl(k->stream_id);
  get_k.timestamp = htonl(k->timestamp);

  key.data = &get_k;
  key.size = key.ulen = sizeof(struct rec_key);
  key.flags = DB_DBT_USERMEM;
  
  data.data = buf;
  data.size = data.ulen = len;
  data.flags = DB_DBT_USERMEM;
  
  if ((ret = cursorp->get(cursorp, &key, &data, flags)) == 0) {
    if (flags & DB_NEXT || flags & DB_SET_RANGE) {
      k->stream_id = ntohl(get_k.stream_id);
      k->timestamp = ntohl(get_k.timestamp);
    }
    return 0;
  }
  warn("Get failed: %s\n", db_strerror(ret));
  return -1;
}

int get_partial(DBC *cursorp, int flags, struct rec_key *k, void *buf, int len, int off) {
  struct rec_key get_k;
  int ret;
  DBT key, data;
  bzero(&key, sizeof(key));
  bzero(&data, sizeof(data));

  debug("get_partial stream_id: %i timestamp: 0x%x\n",
        k->stream_id, k->timestamp);

  get_k.stream_id = htonl(k->stream_id);
  get_k.timestamp = htonl(k->timestamp);

  key.data = &get_k;
  key.size = key.ulen = sizeof(struct rec_key);
  key.flags = DB_DBT_USERMEM;
  
  data.data = buf;
  data.size = data.ulen = data.dlen = len;
  data.doff = off;
  data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

  if ((ret = cursorp->get(cursorp, &key, &data, flags)) == 0) {
    if (flags & DB_NEXT || flags & DB_SET_RANGE) {
      k->stream_id = ntohl(get_k.stream_id);
      k->timestamp = ntohl(get_k.timestamp);
    }
    return 0;
  }
  return ret;
}
