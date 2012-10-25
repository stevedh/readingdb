

#include <time.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <zlib.h>
#include <db.h>
#include <arpa/inet.h>
#include <assert.h>

#include "config.h"
#if HAVE_ENDIAN_H
#include <endian.h>
#elif HAVE_LIBKERN_OSBYTEORDER_H
#include <libkern/OSByteOrder.h>
#define htobe64(X) OSSwapHostToBigInt64(X)
#else
#error "No endian header found"
#endif


#include "readingdb.h"
#include "rpc.h"
#include "pbuf/rdb.pb-c.h"
#include "logging.h"

#define ZLIB_LEVEL Z_HUFFMAN_ONLY

static void _free_db_rec(DatabaseRecord *db) {
  free(db->deltas[0]);
  free(db->deltas);
  if (db->first) free(db->first);
  free(db);
}

static DatabaseRecord * _alloc_db_rec(int n) {
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

static inline int64_t DOUBLE_DELTA(double x, double y) {
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

static inline double DOUBLE_ADD(double x, int64_t delta) {
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

static int pbuf_compress(struct rec_val *v, void *buf, int outbuf) {
  DatabaseRecord *db = _alloc_db_rec(v->n_valid + 1);
  double bottom = LLONG_MIN + 1, top = LLONG_MAX - 1;
  int i;
  uint32_t last_ts = 0;
  int64_t last_delta = 0;
  int pack_size;
  if (!db) return 0;

  // fprintf(stderr, "pbuf_compress: %lu %lu\n", ntohl(k->stream_id), ntohl(k->timestamp));
  db->period_length = v->period_length;
  
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
        last_delta = db->deltas[i-1]->value = 
          DOUBLE_DELTA(v->data[i].reading, v->data[i-1].reading);
        db->deltas[i-1]->has_value = 1;
      }

      if (v->data[i].reading_sequence != 0) {
        db->deltas[i-1]->has_seqno = 1;
        if (v->data[i - 1].reading_sequence != 0)
          db->deltas[i-1]->seqno = v->data[i].reading_sequence - 
            v->data[i-1].reading_sequence;
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


int val_compress(struct rec_val *v, void *buf, int len) {
  char pbuf_buf[COMPRESS_WORKING_BUF];
  int c_sz, ret;

  c_sz = pbuf_compress(v, pbuf_buf, sizeof(pbuf_buf));
  // fprintf(stderr, "compressed-size: %i bound: %i len: %i\n", c_sz, compressBound(c_sz), len);
  if (c_sz < 0) {
    assert(0);
  }
  
  assert(compressBound(c_sz) < len);
  uLongf sz = len;
  if ((ret = compress2(buf, &sz, pbuf_buf, c_sz, ZLIB_LEVEL)) < 0) {
    fatal("compress2 failed: %i (dest: %lu src: %lu bound: %lu)\n", 
          ret, sz, c_sz, compressBound(c_sz));
    assert(0);
  }
  debug("compress2: %i -> %i\n", c_sz, sz);

  return (int)sz;
}

static int pbuf_decompress(void *buf, int len, struct rec_val *val, int sz) {
  DatabaseRecord *rec;
  uint32_t last_ts = 0;
  int64_t last_delta = 0;
  int i, n_present;
  int unpack_sz;
  rec = database_record__unpack(NULL, len, buf);
  unpack_sz = sizeof(struct rec_val) + (sizeof(struct point) * 
                                        ((rec->first == NULL ? 0 : 1) + 
                                         rec->n_deltas));
  
  if (!rec)
    assert(0);

  // fprintf(stderr, "pbuf_decompress: %lu %lu\n", rec->k_streamid, rec->k_timestamp);
  debug("unpack_sz: %i, sz: %i\n", unpack_sz, sz);

  val->period_length = rec->period_length;
  val->n_valid = rec->n_deltas + (rec->first == NULL ? 0 : 1);

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
        val->data[i+1].reading_sequence = val->data[i].reading_sequence + 
          rec->deltas[i]->seqno;
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

int val_decompress(void *cmpr, int cmpr_len, struct rec_val *v, int v_len) {
  char zbuf[COMPRESS_WORKING_BUF];  
  uLongf destLen = sizeof(zbuf);
  int bufsz;
  if (uncompress(zbuf, &destLen, cmpr, cmpr_len) != Z_OK) {
      warn("inflate failed!\n");
      assert(0);
  }

  debug("inflate: %i -> %i\n", cmpr_len, destLen);

  bufsz = pbuf_decompress(zbuf, destLen, v, v_len);
  if (bufsz < 0) {
    assert(0);
  } else {
    return 0;
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
