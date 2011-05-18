
#include <time.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <libkern/OSByteOrder.h>
#include <zlib.h>
#include <db.h>
#include <arpa/inet.h>

#include "readingdb.h"
#include "rpc.h"
#include "pbuf/rdb.pb-c.h"
#include "logging.h"

#define ZLIB_LEVEL Z_HUFFMAN_ONLY

void _free_db_rec(DatabaseRecord *db) {
  free(db->deltas[0]);
  free(db->deltas);
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
  i1 = OSSwapHostToBigInt64(i1);
  i2 = OSSwapHostToBigInt64(i2);

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
  i1 = OSSwapHostToBigInt64(i1);
  i1 += delta;
  i1 = OSSwapHostToBigInt64(i1);
  // pi(i1), printf("\n");
  memcpy(&ret, &i1, sizeof(ret));
  return ret;
}

// #define DOUBLE_DELTA(X,Y)  (((uint64_t)((X) - (Y))))
#define HAS_MAX(X) ((X) < ((double)(LLONG_MAX - 1)))
#define HAS_MIN(X) ((X) < ((double)(LLONG_MIN + 1)))

int pbuf_compress(struct rec_key *k,
                  struct point *d, int len, 
                  void *buf, int outbuf) {
  DatabaseRecord *db = _alloc_db_rec(len - 1);
  double bottom = LLONG_MIN + 1, top = LLONG_MAX - 1;
  int i;
  uint32_t last_ts = 0;
  int64_t last_delta = 0;
  int pack_size;
  if (!db) return 0;

  db->k_streamid = ntohs(k->stream_id);
  db->k_timestamp = ntohs(k->timestamp);

  db->first->timestamp = d[0].timestamp;
  db->first->value = d[0].reading;

  if (d[0].reading_sequence != 0) {
    db->first->has_seqno = 1;
    db->first->seqno = d[0].reading_sequence;
  }
  if (d[0].min > bottom) {
    db->first->has_min = 1;
    db->first->min = d[0].min;
  }
  if (d[0].max < top) {
    db->first->has_max = 1;
    db->first->max = d[0].max;
  }

  for (i = 1; i < len; i++) {
    /* include timestamp deltas if they're different */
    if (d[i].timestamp - d[i-1].timestamp != last_ts) {
      last_ts = db->deltas[i-1]->timestamp = d[i].timestamp - d[i-1].timestamp;
      db->deltas[i-1]->has_timestamp = 1;
    }

    /* the reading is also packed if we're not the same */
    if (DOUBLE_DELTA(d[i].reading, d[i-1].reading) != last_delta) {
      last_delta = db->deltas[i-1]->value = DOUBLE_DELTA(d[i].reading, d[i-1].reading);
      db->deltas[i-1]->has_value = 1;
    }

    if (d[i].reading_sequence != 0) {
      db->deltas[i-1]->has_seqno = 1;
      if (d[i - 1].reading_sequence != 0)
        db->deltas[i-1]->seqno = d[i].reading_sequence - d[i-1].reading_sequence;
      else
        db->deltas[i-1]->seqno = d[i].reading_sequence;
    }

    /* include deltas or full readings for min and max */
    if (HAS_MIN(d[i].min)) {
      if (HAS_MIN(d[i-1].min)) {
        printf("HAS MIN");
        db->deltas[i-1]->min_delta = DOUBLE_DELTA(d[i].min, d[i-1].min);
        db->deltas[i-1]->has_min_delta = 1;
      } else {
        db->deltas[i-1]->min = d[i].min;
        db->deltas[i-1]->has_min = 1;
      }
    }
    if (HAS_MAX(d[i].max)) {
      if (HAS_MAX(d[i-1].max)) {
        printf("HAS MAX\n");
        db->deltas[i-1]->max_delta = DOUBLE_DELTA(d[i].max, d[i-1].max);
        db->deltas[i-1]->has_max_delta = 1;
      } else {
        db->deltas[i-1]->max = d[i].max;
        db->deltas[i-1]->has_max = 1;
      }
    }
    

/*     printf("%i %i %i %i %i\n",  */
/*            db->deltas[i-1]->has_seqno, */
/*            db->deltas[i-1]->has_min_delta, */
/*            db->deltas[i-1]->has_max_delta, */
/*            db->deltas[i-1]->has_min, */
/*            db->deltas[i-1]->has_max); */
  }

  pack_size = database_record__get_packed_size(db);
  if (outbuf < pack_size) {
    _free_db_rec(db);
    return - pack_size;
  }
  database_record__pack(db, buf);
  _free_db_rec(db);
  return pack_size;

/*   FILE *fp = fopen("out", "wb"); */
/*   if (!fp) perror("fopen"); */
/*   int sz = database_record__get_packed_size(db); */
/*   buf = malloc(sz); */
/*   uLongf sz2 = compressBound(sizeof(struct point) * len); */
/*   printf("point: %lu (%i)\n", sizeof(struct point) * len, len); */
/*   printf("pbuf: %i (%f)\n", sz, (float)sz / len); */

/*   void *buf2 = malloc(compressBound(sz)); */
/*   int sz3 = compress2(buf2, &sz2, d, sizeof(struct point) * len, ZLIB_LEVEL); */
/*   printf("deflate: %lu (%f)\n", sz2, (float)sz2 / len); */

/*   sz2 = compressBound(sz); */
/*   compress2(buf2, &sz2, buf, sz, ZLIB_LEVEL); */
/*   printf("pbuf + deflate: %lu (%f)\n", sz2, (float)sz2 / len); */

/*   return sz2; */
/*   // fwrite(buf, sz, 1, fp); */
/*   // fclose(fp); */
/*   free(buf); */
/*   free(buf2); */
/*   return sz; */
}


int bdb_compress(DB *dbp, const DBT *prevKey, const DBT *prevData, 
                 const DBT *key, const DBT *data, DBT *dest) {
  char pbuf_buf[64000];
  info("bdb_compress -- called : %i %i\n", key->size, data->size);
  assert(key->size == sizeof(struct rec_key));
  pbuf_compress((struct rec_key *)key->data, 
                data->data, data->size,
                pbuf_buf, sizeof(pbuf_buf));
}

int pbuf_decompress(void *buf, int len, struct point *p, int n) {
  DatabaseRecord *rec;
  uint32_t last_ts = 0;
  int64_t last_delta = 0;
  int i;

  rec = database_record__unpack(NULL, len, buf);
  if (!rec || rec->n_deltas > n) return 0;
  _rpc_copy_records(&p[0], &rec->first, 1);

  for (i = 0; i < rec->n_deltas; i++) {
    if (rec->deltas[i]->has_timestamp) {
      last_ts = rec->deltas[i]->timestamp;
    }
    p[i+1].timestamp = p[i].timestamp + last_ts;

    if (rec->deltas[i]->has_value) {
      last_delta = rec->deltas[i]->value;
    }
    p[i+1].reading = DOUBLE_ADD(p[i].reading, last_delta);

    if (rec->deltas[i]->has_seqno) {
      if (p[i].reading_sequence == 0) {
        p[i+1].reading_sequence = rec->deltas[i]->seqno;
      } else {
        p[i+1].reading_sequence = p[i].reading_sequence + rec->deltas[i]->seqno;
      }
    } else {
      p[i+1].reading_sequence = 0;
    }
    
    p[i+1].min = LLONG_MIN;
    p[i+1].max = LLONG_MAX;
  }
  database_record__free_unpacked(rec, NULL);
  return rec->n_deltas + 1;
}

int bdb_decompress(DB *dbp, const DBT *prevKey, const DBT *prevData, 
                   DBT *compressed, DBT *destKey, DBT *destData) {
  info("decompress -- called!");
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
