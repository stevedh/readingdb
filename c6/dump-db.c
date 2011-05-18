
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <db.h>

#include "readingdb.h"
#include "util.h"
#include "logging.h"

void usage(char *progname) {
  fprintf(stderr, "\n  %s [options] <dbfile>\n"
          "    -b     dump buckets\n"
          "    -d     dump data\n"
          "    -s <s> dump only stream s\n"
          "    -h     help\n\n", progname);
}

int main(int argc, char **argv) {
  struct rec_key k;
  struct rec_val *v;
  char buf[SHM_SIZ];
  int ret, req_stream = -1, prev_stream = -1;
  DB *dbp;
  DBC *cursorp;
  int dump_buckets = 0, dump_data = 0, read_size = sizeof(struct rec_val), c;
  char *dbpath = NULL;
  int stream_recs = -1;

  v = (struct rec_val *)buf;

  log_init();
  log_setlevel(LOGLVL_INFO);

  while ((c = getopt(argc, argv, "bdhs:")) != -1) {
    switch (c) {
    case 'b':
      dump_buckets = 1;
      break;
    case 'd':
      dump_data = 1;
      /* need to read the whole record  */
      read_size = sizeof(buf);
      break;
    case 's':
      req_stream = atoi(optarg);
      info("Dumping only stream %i\n", req_stream);
      break;
    case 'h':
      usage(argv[0]);
      return 1;
    }
  }

  if (optind != argc - 1) {
    usage(argv[0]);
    return 1;
  } else {
    dbpath = argv[optind];
  }

  if ((ret = db_create(&dbp, NULL, 0)) != 0) {
    fatal("db_create: %s\n", db_strerror(ret));
    return 1;
  }

  if ((ret = dbp->open(dbp,
                       NULL, dbpath, NULL, DB_BTREE, DB_RDONLY, 0644)) != 0) {
    fatal("db_open: %s\n", db_strerror(ret));
    return 1;
  }

  ret = dbp->cursor(dbp, NULL, &cursorp, 0);
  if (cursorp == NULL) {
    fatal("cursor: %s\n", db_strerror(ret));
    goto done;
  }

  memset(&k, 0, sizeof(k));
  if (req_stream > 0) {
    k.stream_id = req_stream;
  }

  if ((ret = get_partial(cursorp, DB_SET_RANGE, &k, buf, read_size, 0)) < 0) {
    error("Error seeking in database: %s\n", db_strerror(ret));
    goto done;
  }
  
  do {
    int i;
    if (req_stream >= 0 && k.stream_id != req_stream) break;
    if (k.stream_id != prev_stream) {
      if (dump_buckets || dump_data) {
        printf("\nStream %i\n", k.stream_id);
      } else {
        if (stream_recs >= 0) {
          printf("%i\n", stream_recs);
          stream_recs = 0;
        }
        printf("%i\t", k.stream_id);
      }
      prev_stream = k.stream_id;
    }
    stream_recs += v->n_valid;
    if (dump_buckets) 
      printf("%-10i %-10i %-10i %-10i\n", k.timestamp, v->tail_timestamp, v->period_length, v->n_valid);
    if (dump_data)
      for (i = 0; i < v->n_valid; i++) {
        printf("%i\t%i\t%f\t%f\t%f\n", 
               v->data[i].timestamp,
               v->data[i].reading_sequence,
               v->data[i].reading,
               v->data[i].min,
               v->data[i].max);
      }
  } while (get_partial(cursorp, DB_NEXT, &k, buf, read_size, 0) == 0);

done:
  info("Done: closing and exiting\n");
  cursorp->close(cursorp);
  dbp->close(dbp, 0);
  return 0;
}
