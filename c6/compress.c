
#include <time.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
//#include <libkern/OSByteOrder.h>
#include <endian.h>
#include <zlib.h>
#include <db.h>
#include <arpa/inet.h>
#include <assert.h>

#include "readingdb.h"
#include "rpc.h"
#include "pbuf/rdb.pb-c.h"
#include "logging.h"

#define ZLIB_LEVEL Z_HUFFMAN_ONLY

void _free_db_rec(DatabaseRecord *db) {
  free(db->deltas[0]);
  free(db->deltas);
  if (db->first) free(db->first);
  free(db);
}

DatabaseRecord * _alloc_db_rec(int n) {
  DatabaseRecord *rec;
  DatabaseDelta *deltas;
  DatabaseDelta **deltas_vec;
  Reading *reading;
  int i;

  rec = malloc(sizeof(DatabaseRecord));
  deltas = malloc(sizeof(DatabaseDelta) * n);
  deltas_vec = malloc(sizeof(DatabaseDelta *) * n);
  reading = malloc(sizeof(Reading));
  if (!rec || !deltas || !deltas_vec || !reading) {
    if (rec) free(rec);
    if (deltas) free(deltas);
    if (deltas_vec) free(deltas_vec);
    if (reading) free(reading);
    return NULL;
  }

  database_record__init(rec);
  reading__init(reading);
  rec->first = reading;
  rec->deltas = deltas_vec;
  rec->n_deltas = n;
  
  for (i = 0; i < n; i++) {
    database_delta__init(&deltas[i]);
    deltas_vec[i] = &deltas[i];
  }
  return rec;
}

inline int64_t DOUBLE_DELTA(double x, double y) {
  int64_t i1, i2;
  memcpy(&i1, &x, sizeof(i1));
  memcpy(&i2, &y, sizeof(i2));
  i1 = htobe64(i1);
  i2 = htobe64(i2);

  // pf(x), pf(y), printf("\n");
  // pi(i1 - i2), printf("\n");

  return i1 - i2;
}

void pf(double delta) {
  int i;
  for (i = 0; i < 8; i++) {
    printf("%02x ", ((uint8_t *)&delta)[i]);
  }
}

void pi(uint64_t delta) {
  int i;
  for (i = 0; i < 8; i++) {
    printf("%02x ", ((uint8_t *)&delta)[i]);
  }
}

inline double DOUBLE_ADD(double x, int64_t delta) {
  int64_t i1;
  double ret;
  memcpy(&i1, &x, sizeof(i1));
  // pi(i1), pi(delta), printf("\n");
  i1 = htobe64(i1);
  i1 += delta;
  i1 = htobe64(i1);
  // pi(i1), printf("\n");
  memcpy(&ret, &i1, sizeof(ret));
  return ret;
}

// #define DOUBLE_DELTA(X,Y)  (((uint64_t)((X) - (Y))))
#define HAS_MAX(X) ((X) < ((double)(LLONG_MAX - 1)))
#define HAS_MIN(X) ((X) < ((double)(LLONG_MIN + 1)))

int pbuf_compress(struct rec_key *k,
                  struct rec_val *v, 
                  void *buf, int outbuf) {
  DatabaseRecord *db = _alloc_db_rec(v->n_valid + 1);
  double bottom = LLONG_MIN + 1, top = LLONG_MAX - 1;
  int i;
  uint32_t last_ts = 0;
  int64_t last_delta = 0;
  int pack_size;
  if (!db) return 0;

  // fprintf(stderr, "pbuf_compress: %lu %lu\n", ntohl(k->stream_id), ntohl(k->timestamp));

  db->k_streamid = ntohl(k->stream_id);
  db->k_timestamp = ntohl(k->timestamp);
  db->period_length = v->period_length;
  db->n_valid = v->n_valid;
  
  // printf("here:, n_valid: %i\n", v->n_valid);

  if (v->n_valid == 0) {
    free(db->first);
    db->first = NULL;
    db->n_deltas = 0;
  } else {
    db->n_deltas = v->n_valid - 1;
    db->first->timestamp = v->data[0].timestamp;
    db->first->value = v->data[0].reading;
    
    if (v->data[0].reading_sequence != 0) {
      db->first->has_seqno = 1;
      db->first->seqno = v->data[0].reading_sequence;
    }
    if (v->data[0].min > bottom) {
      db->first->has_min = 1;
      db->first->min = v->data[0].min;
    }
    if (v->data[0].max < top) {
      db->first->has_max = 1;
      db->first->max = v->data[0].max;
    }

    for (i = 1; i < v->n_valid; i++) {
      /* include timestamp deltas if they're different */
      if (v->data[i].timestamp - v->data[i-1].timestamp != last_ts) {
        last_ts = db->deltas[i-1]->timestamp = v->data[i].timestamp - v->data[i-1].timestamp;
        db->deltas[i-1]->has_timestamp = 1;
      }

      /* the reading is also packed if we're not the same */
      if (DOUBLE_DELTA(v->data[i].reading, v->data[i-1].reading) != last_delta) {
        last_delta = db->deltas[i-1]->value = DOUBLE_DELTA(v->data[i].reading, v->data[i-1].reading);
        db->deltas[i-1]->has_value = 1;
      }

      if (v->data[i].reading_sequence != 0) {
        db->deltas[i-1]->has_seqno = 1;
        if (v->data[i - 1].reading_sequence != 0)
          db->deltas[i-1]->seqno = v->data[i].reading_sequence - v->data[i-1].reading_sequence;
        else
          db->deltas[i-1]->seqno = v->data[i].reading_sequence;
      }

      /* include deltas or full readings for min and max */
      if (HAS_MIN(v->data[i].min)) {
        if (HAS_MIN(v->data[i-1].min)) {
          // printf("HAS MIN");
          db->deltas[i-1]->min_delta = DOUBLE_DELTA(v->data[i].min, v->data[i-1].min);
          db->deltas[i-1]->has_min_delta = 1;
        } else {
          db->deltas[i-1]->min = v->data[i].min;
          db->deltas[i-1]->has_min = 1;
        }
      }
      if (HAS_MAX(v->data[i].max)) {
        if (HAS_MAX(v->data[i-1].max)) {
          // printf("HAS MAX\n");
          db->deltas[i-1]->max_delta = DOUBLE_DELTA(v->data[i].max, v->data[i-1].max);
          db->deltas[i-1]->has_max_delta = 1;
        } else {
          db->deltas[i-1]->max = v->data[i].max;
          db->deltas[i-1]->has_max = 1;
        }
      }
    }
  }

  pack_size = database_record__get_packed_size(db);
  if (outbuf < pack_size) {
    _free_db_rec(db);
    return - pack_size;
  }
  database_record__pack(db, buf);
  _free_db_rec(db);
  return pack_size;
}


int bdb_compress(DB *dbp, const DBT *prevKey, const DBT *prevData, 
                 const DBT *key, const DBT *data, DBT *dest) {
  char pbuf_buf[64000];
  int c_sz;
  struct rec_key *k = (struct rec_key *)key->data;
  struct rec_val *v = (struct rec_val *)data->data;
  struct cmpr_rec_header *hdr;
  //fprintf(stderr, "bdb_compress -- called : %i %i\n", key->size, data->size);
  assert(key->size == sizeof(struct rec_key));
/*   printf("%i\n", ntohl(k->timestamp)); */
  // printf("%i %i %i\n", data->size, v->n_valid, v->n_valid);
/*   printf("%i\n", data->ulen); */
  assert(data->size == sizeof(struct rec_val) + (sizeof(struct point) * v->n_valid));
  c_sz = pbuf_compress((struct rec_key *)key->data, 
                       (struct rec_val *)data->data,
                       pbuf_buf, sizeof(pbuf_buf));
  // fprintf(stderr, "compressed-size: %i\n", c_sz);
  if (c_sz < 0) {
    assert(0);
  }
  
  if (dest->ulen < compressBound(c_sz)) {
    warn("Not compressing because %i < %i\n", dest->ulen, compressBound(c_sz));
    dest->size = compressBound(c_sz) + sizeof(struct cmpr_rec_header);
    return DB_BUFFER_SMALL;
  } else {
    // uint32_t *unpack_sz = (uint32_t *)dest->data;
    hdr = dest->data;
    //printf("%llu\n", (uint64_t)dest->data);
    // assert( (uint64_t)dest->data % 4 == 0);
    // assert(dest->data == (uint32_t *)dest->data);
    uLongf sz = dest->ulen - sizeof(struct cmpr_rec_header);

    if (compress2(((char *)dest->data) + sizeof(struct cmpr_rec_header), &sz, pbuf_buf, c_sz, ZLIB_LEVEL) < 0) {
      assert(0);
    }
    dest->size = sz + sizeof(struct cmpr_rec_header);
    hdr->uncompressed_len = htonl(c_sz);
    hdr->compressed_len = htonl(sz);
    // info("final compressed sz is %i\n", dest->size);
    debug("compress: streamid: %i timestamp: 0x%x n_valid: %i %lu -> %lu\n", 
         ntohl(k->stream_id), ntohl(k->timestamp), 
         v->n_valid, data->size, sz);
    return 0;
  }
  assert(0);
}

int pbuf_decompress(struct rec_key *key, void *buf, int len, struct rec_val *val, int sz) {
  DatabaseRecord *rec;
  uint32_t last_ts = 0;
  int64_t last_delta = 0;
  int i, n_present;
  int unpack_sz;
  rec = database_record__unpack(NULL, len, buf);
  unpack_sz = sizeof(struct rec_val) + (sizeof(struct point) * ((rec->first == NULL ? 0 : 1) + rec->n_deltas));

  if (!rec || 
      sz < unpack_sz) {
    database_record__free_unpacked(rec, NULL);
    return - unpack_sz;
  }
  key->stream_id = htonl(rec->k_streamid);
  key->timestamp = htonl(rec->k_timestamp);
  // fprintf(stderr, "pbuf_decompress: %lu %lu\n", rec->k_streamid, rec->k_timestamp);
  val->period_length = rec->period_length;
  val->n_valid = rec->n_valid; //rec->n_deltas + (rec->first == NULL ? 0 : 1);

  //  printf("read back n_valid: %i\n", val->n_valid);

  if (val->n_valid == 0) {
    database_record__free_unpacked(rec, NULL);
    return unpack_sz;
  }

  /* if there's at least one record first is present */
  _rpc_copy_records(&val->data[0], &rec->first, 1);

  for (i = 0; i < rec->n_deltas; i++) {
    if (rec->deltas[i]->has_timestamp) {
      last_ts = rec->deltas[i]->timestamp;
    }
    val->data[i+1].timestamp = val->data[i].timestamp + last_ts;

    if (rec->deltas[i]->has_value) {
      last_delta = rec->deltas[i]->value;
    }
    val->data[i+1].reading = DOUBLE_ADD(val->data[i].reading, last_delta);

    if (rec->deltas[i]->has_seqno) {
      if (val->data[i].reading_sequence == 0) {
        val->data[i+1].reading_sequence = rec->deltas[i]->seqno;
      } else {
        val->data[i+1].reading_sequence = val->data[i].reading_sequence + rec->deltas[i]->seqno;
      }
    } else {
      val->data[i+1].reading_sequence = 0;
    }
    
    val->data[i+1].min = LLONG_MIN;
    val->data[i+1].max = LLONG_MAX;
  }
  val->tail_timestamp = val->data[i].timestamp;
  database_record__free_unpacked(rec, NULL);
  return unpack_sz;
}

int bdb_decompress(DB *dbp, const DBT *prevKey, const DBT *prevData, 
                   DBT *compressed, DBT *destKey, DBT *destData) {
  char zbuf[64000];  
  struct cmpr_rec_header *hdr;
  struct rec_key *k = destKey->data;
  struct rec_val *v = destData->data;
  uLongf destLen;
  assert(compressed->size >= sizeof(struct cmpr_rec_header));

  destKey->size = sizeof(struct rec_key);
  // printf("destkey %i %i\n", destKey->size, destKey->ulen);
  if (destKey->size > destKey->ulen) 
    return DB_BUFFER_SMALL;

  hdr = compressed->data;
  if (ntohl(hdr->uncompressed_len) < sizeof(zbuf)) {
    int bufsz;
    destLen = ntohl(hdr->uncompressed_len);
    if (uncompress(zbuf, &destLen, 
                   ((char *)compressed->data) + sizeof(sizeof(struct cmpr_rec_header)), 
                   ntohl(hdr->compressed_len)) != Z_OK) {
      warn("inflate failed!\n");
      assert(0);
    }
    bufsz = pbuf_decompress((struct rec_key *)destKey->data, 
                            zbuf, destLen, destData->data, 
                            destData->ulen);
    debug("contained %i points\n", bufsz);
    if (bufsz < 0) {
      destData->size = - bufsz;
      return DB_BUFFER_SMALL;
    } else {
      destData->size = bufsz;
      debug("decompress: streamid: %i timestamp: 0x%x n_valid: %i, %u -> %u\n", 
           ntohl(k->stream_id), ntohl(k->timestamp), v->n_valid,
           compressed->size, bufsz);

      compressed->size = (ntohl(hdr->compressed_len) + sizeof(struct cmpr_rec_header));
      return 0;
    }
  }
  assert(0);
}


#if 0
int main(int argc, char **argv) {
#define NPTS 10000
  struct point p[NPTS];
  int i = 0;
  memset(p, 0, sizeof(p));

  FILE *fp = fopen(argv[1], "r");
  while (i < NPTS && fscanf(fp, "%lu,%lf\n", &p[i].timestamp, &p[i].reading) == 2) {
    p[i].reading_sequence = i;
    p[i].max = LLONG_MAX;
    p[i].min = LLONG_MIN;
    i++;
  }
  fclose(fp);

  struct timeval start, end, delta;
  gettimeofday(&start, NULL);

  char cbuf[300000];
  unsigned int sz = pbuf_compress(p, i, cbuf, sizeof(cbuf));
  //gettimeofday(&end, NULL);
  //timeval_subtract(&delta, &end, &start);
  //printf("pbuf compress: %i.%06i\n", delta.tv_sec, delta.tv_usec);

  //gettimeofday(&start, NULL);
  // printf("read %i\n", i);
  uLongf gzsz = compressBound(sz);
  void *gzbuf = malloc(gzsz);

  compress2(gzbuf, &gzsz, cbuf, sz, ZLIB_LEVEL);
  free(gzbuf);
  gettimeofday(&end, NULL);
  timeval_subtract(&delta, &end, &start);
  printf("deflate compress: %i.%06i\n", delta.tv_sec, delta.tv_usec);

  printf("%u %i %i %.04f %04f %.02f\n", 
         sizeof(struct point) * i, 
         i * 12,
         sz, 
         (float)sz / (sizeof(struct point) * i), 
         (float)sz / (i * 12),
         (float)sz / i);

  printf("%u %i %i %.04f %04f %.02f\n", 
         sizeof(struct point) * i, 
         i * 12,
         gzsz, 
         (float)gzsz / (sizeof(struct point) * i), 
         (float)gzsz / (i * 12),
         (float)gzsz / i);


  struct point recon[NPTS];
  int read= 
    pbuf_decompress(cbuf, sz, recon, NPTS);
  // printf("%i\n", read);
  assert(read == i);



  int j;
  ReadingSet *rs = _rpc_alloc_rs(i);
  for (j = 0; j < i; j++) {
    reading__init(rs->data[j]);
    _rpc_copy_reading(rs->data[j], &p[j]);
  }
  rs->n_data = i;
  void * simple; int simple_len;
  simple_len = reading_set__get_packed_size(rs);
  simple = malloc(simple_len);
  printf("simple len: %i\n", simple_len);
  reading_set__pack(rs, simple);

  gzsz = compressBound(simple_len);
  gzbuf = malloc(gzsz);

  compress2(gzbuf, &gzsz, simple, simple_len, 0);
  printf("%u %i %i %.04f %04f %.02f\n", 
         sizeof(struct point) * i, 
         i * 12,
         gzsz, 
         (float)gzsz / (sizeof(struct point) * i), 
         (float)gzsz / (i * 12),
         (float)gzsz / i);


/*   for (i = 0; i < 20; i++) { */
/*     printf("%lu\t%lu\t%lf\t\t%lu\t%lu\t%lf\n",  */
/*            p[i].timestamp, p[i].reading_sequence, p[i].reading, */
/*            recon[i].timestamp, p[i].reading_sequence, recon[i].reading); */
/*   } */

  assert(memcmp(recon, p, sizeof(struct point) * read) == 0);
  return 0;
}
#endif
