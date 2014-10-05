
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <db.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/select.h>
#include <fcntl.h>
#include <semaphore.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <assert.h>
#include <pthread.h>
#include <limits.h>

#include "readingdb.h"
#include "util.h"
#include "logging.h"
#include "hashtable.h"
#include "stats.h"
#include "rpc.h"
#include "pbuf/rdb.pb-c.h"
#include "commands.h"
#include "config.h"
#include "rdb.h"
#include "sketch.h"

struct config conf;
extern struct subdb dbs[MAX_SUBSTREAMS];

sig_atomic_t do_shutdown = 0;
void sig_shutdown(int arg) {
  do_shutdown = 1;
}
void signal_setup() {
  sigset_t old, set;
  signal(SIGINT, sig_shutdown);

  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  sigprocmask(SIG_BLOCK, &set, &old);
}

FREELIST(struct ipc_command, dirty_data);

/* concurrency limit and wait for workers to exit */
pthread_mutex_t worker_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t worker_cond = PTHREAD_COND_INITIALIZER;
int worker_count = 0;
sem_t worker_count_sem;

#define WORKER_ADD     {   sem_wait(&worker_count_sem);        \
pthread_mutex_lock(&worker_lock);                              \
    worker_count ++;                                           \
    pthread_mutex_unlock(&worker_lock); }

#define WORKER_REMOVE  { pthread_mutex_lock(&worker_lock);     \
    worker_count --;                                           \
    pthread_cond_broadcast(&worker_cond);                      \
    pthread_mutex_unlock(&worker_lock);                        \
    sem_post(&worker_count_sem); }

#define WORKER_WAIT { pthread_mutex_lock(&worker_lock);        \
    info("waiting for %i clients to finish\n", worker_count);  \
    while (worker_count > 0)                                   \
      pthread_cond_wait(&worker_cond, &worker_lock);           \
    pthread_mutex_unlock(&worker_lock); }

pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;
struct stats stats = {0, 0, 0, 0};


void usage(char *progname) {
  fprintf(stderr, 
          "\n\t%s [options]\n"
          "\t\t-V                 print version and exit\n"
          "\t\t-v                 verbose\n"
          "\t\t-h                 help\n"
          "\t\t-d <datadir>       set data directory (%s)\n"
          "\t\t-c <interval>      set commit interval (10s)\n"
          "\t\t-p <port>          local port to bind to (4242)\n"
          "\t\t-l <interval>      how often to run the deadlock detector (2s)\n"
          "\t\t-a <interval>      how often to checkpoint and archive (300s)\n"
          "\t\t-s <cache size>    cache size (32MB)\n"
          "\t\t-r                 enable support for resampling\n\n",
          progname, DATA_DIR);
}

#define INVALID_INT_ARG(ARG) ((errno == ERANGE && \
           ((ARG) == LONG_MAX ||                  \
            (ARG) == LONG_MIN)) ||                \
          (errno != 0 && (ARG) == 0) ||           \
          endptr == optarg)

void default_config(struct config *c) {
  char *cur;
  c->loglevel = LOGLVL_INFO;
  strcpy(c->data_dir, DATA_DIR);
  cur = stpncpy(c->sketch_log, c->data_dir, sizeof(c->sketch_log) - 1);
  *cur++ = '/';
  stpncpy(cur, DIRTY_SKETCH_LOGFILE, 
          sizeof(c->sketch_log) - (cur - c->sketch_log));

  c->port = 4242;
  c->cache_size = 32;
  c->commit_interval = 10;
  c->deadlock_interval = 2;
  c->checkpoint_interval = 300;
  c->sketch = 0;
}

int optparse(int argc, char **argv, struct config *c) {
  char o;
  char *endptr, *cur;
  while ((o = getopt(argc, argv, "Vvhd:c:p:s:l:a:r")) != -1) {
    switch (o) {
    case 'h':
      usage(argv[0]);
      return -1;
      break;
    case 'V':
      printf("%s\n", PACKAGE_STRING);
      return -1;
    case 'v':
      c->loglevel = LOGLVL_DEBUG;
      break;
    case 'd':
      strncpy(c->data_dir, optarg, FILENAME_MAX);
      cur = stpncpy(c->sketch_log, optarg, FILENAME_MAX);
      *cur++ = '/';
      stpncpy(cur, DIRTY_SKETCH_LOGFILE, sizeof(c->sketch_log) - (cur - c->sketch_log));
      break;
    case 's':
      c->cache_size = strtol(optarg, &endptr, 10);
      if (endptr == optarg) {
        fatal("Invalid cache size\n");
        return -1;
      }
      break;
    case 'c':
      c->commit_interval = strtol(optarg, &endptr, 10);
      if (INVALID_INT_ARG(c->commit_interval)) {
        fatal("Invalid commit interval\n");
        return -1;
      }
      break;
    case 'l':
      c->deadlock_interval = strtol(optarg, &endptr, 10);
      if (INVALID_INT_ARG(c->deadlock_interval)) {
        fatal("Invalid deadlock interval\n");
        return -1;
      }
      break;
    case 'a':
      c->checkpoint_interval = strtol(optarg, &endptr, 10);
      if (INVALID_INT_ARG(c->checkpoint_interval)) {
        fatal("Invalid deadlock interval\n");
        return -1;
      }
      break;
    case 'p':
      c->port = strtol(optarg, &endptr, 10);
      if (c->port < 1024) { //  || c->port > 0xffff) {
        fatal("Invalid port\n");
        return -1;
      }
      break;
    case 'r':
      c->sketch = 1;
    }
  }

  info("Commit interval is %i\n", c->commit_interval);
  info("Deadlock interval is %i\n", c->deadlock_interval);
  info("Checkpoint/archive interval is %i\n", c->checkpoint_interval);
  return 0;
}

void process_pbuf(struct sock_request *request) {
  struct pbuf_header h;
  int current_alloc = 0;
  int substream;
  void *buf;
  Query *q = NULL;
  ReadingSet *rs = NULL;
  Nearest *n = NULL;
  Delete *d = NULL;
  Response response = RESPONSE__INIT;

  while (fread(&h, sizeof(h), 1, request->sock_fp) > 0) {
    if (ntohl(h.body_length) > MAX_PBUF_MESSAGE) 
      goto abort;
    if (ntohl(h.body_length) > current_alloc) {
      if (current_alloc > 0) {
	free(buf);
	current_alloc = 0;
      }

      buf = malloc(ntohl(h.body_length));
      if (buf == NULL)
        goto abort;

      current_alloc = ntohl(h.body_length);
    }
    if (fread(buf, ntohl(h.body_length), 1, request->sock_fp) < 0)
      goto abort;

    switch (ntohl(h.message_type)) {
    case MESSAGE_TYPE__QUERY:
      q = query__unpack(NULL, ntohl(h.body_length), buf);
      if (q == NULL) goto abort;
      if (q->substream >= MAX_SUBSTREAMS) {
        query__free_unpacked(q, NULL);
        goto abort;
      } 
      if (q->substream == 0 && 
          q->sketch &&
          get_sketch_substream(q->sketch) > 0) {
        substream = get_sketch_substream(q->sketch);
        /* invalid substream is an error */
        if (substream < 0) {
          response.error = RESPONSE__ERROR_CODE__FAIL_PARAM;
          rpc_send_reply(request, &response);
          query__free_unpacked(q, NULL);
          warn("invalid sketch substream\n");
          goto abort;
        }
        info("returning substream %i for sketch\n", substream);
      } else {
        substream = q->substream;
      }
      INCR_STAT(queries);
      debug("query streamid: %i substream: %i start: %i end: %i\n",
           q->streamid, substream, q->starttime, q->endtime);
      response.error = RESPONSE__ERROR_CODE__OK;
      response.data = _rpc_alloc_rs(MAXRECS);
      if (!response.data) {
        response.error = RESPONSE__ERROR_CODE__FAIL_MEM;
        rpc_send_reply(request, &response);
        query__free_unpacked(q, NULL);
        goto abort;
      }

      response.data->streamid = q->streamid;
      response.data->substream = substream;
      response.data->n_data = 0;

      if (q->has_action) {
        query(dbs[substream].dbp, q, &response, q->action); 
      } else {
        query(dbs[substream].dbp, q, &response, QUERY_DATA); 
      }

      rpc_send_reply(request, &response);
      query__free_unpacked(q, NULL);
      _rpc_free_rs(response.data);
      break;
    case MESSAGE_TYPE__READINGSET:
      rs = reading_set__unpack(NULL, ntohl(h.body_length), buf);
      if (!rs) goto abort;
      if (rs->substream >= MAX_SUBSTREAMS ||
          rs->n_data > MAXRECS) {
        response.error = RESPONSE__ERROR_CODE__FAIL_PARAM;
        rpc_send_reply(request, &response);
        reading_set__free_unpacked(rs, NULL);
        goto q_abort;
      }
      INCR_STAT(adds);
      add_enqueue(&conf, rs, &response);
      reading_set__free_unpacked(rs, NULL);
      break;
    q_abort:
      reading_set__free_unpacked(rs, NULL);
      goto abort;
    case MESSAGE_TYPE__NEAREST:
      debug("Processing nearest command\n");
      n = nearest__unpack(NULL, ntohl(h.body_length), buf);
      if (!n) goto abort;
      if (n->substream >= MAX_SUBSTREAMS) {
        response.error = RESPONSE__ERROR_CODE__FAIL_PARAM;
        rpc_send_reply(request, &response);
        nearest__free_unpacked(n, NULL);
        goto abort;
      }
      response.error = RESPONSE__ERROR_CODE__FAIL_PARAM;
      response.data = _rpc_alloc_rs(n->has_n ? min(n->n, MAXRECS) : 1);
      if (!response.data) {
        response.error = RESPONSE__ERROR_CODE__FAIL_MEM;
        rpc_send_reply(request, &response);
        nearest__free_unpacked(n, NULL);
        goto abort;
      }
      response.data->streamid = n->streamid;
      response.data->substream = n->substream;
      response.data->n_data = 0;
      query_nearest(dbs[n->substream].dbp, n, &response, QUERY_DATA);
      rpc_send_reply(request, &response);
      nearest__free_unpacked(n, NULL);
      _rpc_free_rs(response.data);
      INCR_STAT(nearest);
      break;
    case MESSAGE_TYPE__DELETE:
      debug("Processing delete command\n");
      d = delete__unpack(NULL, ntohl(h.body_length), buf);
      del(dbs[d->substream].dbp, d);
      delete__free_unpacked(d, NULL);
      INCR_STAT(deletes);
      break;
    default:
      response.error = RESPONSE__ERROR_CODE__FAIL_COMMAND;
      rpc_send_reply(request, &response);
      break;
    }
  }
 abort:
  if (current_alloc > 0)
    free(buf);
}

void *process_request(void *request) {
  struct sock_request *req = (struct sock_request *)request;
  struct timeval timeout;
  FILE *fp = NULL;
#if 0
  char buf[4096];
  int dbid = 0;
  ReadingSet *rs = NULL;
  Response resp = RESPONSE__INIT;
#endif
  
  timeout.tv_sec = 60;
  timeout.tv_usec = 0;  
  if (setsockopt(req->sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0)
    goto done;

  fp = fdopen(req->sock, "r+");
  if (!fp)
    goto done;
  else
    req->sock_fp = fp;

  process_pbuf(req);

#if 0
  while (fgets(buf, sizeof(buf), fp) != NULL) {
    if (memcmp(buf, "echo", 4) == 0) {
      fwrite(buf, strlen(buf), 1, fp);
    } else if (memcmp(buf, "help", 4) == 0) {
      char *msg = "echo <msg>\n"
        "help\n"
        "dbid <streamid>\n"
        "put <id> <timestamp> <seqno> <value> [min] [max]\n"
        "get <id> <start> <end>\n";
      fwrite(msg, strlen(msg), 1, fp);
    } else if (memcmp(buf, "quit", 4) == 0) {
      break;
    } else if (memcmp(buf, "dbid", 4) == 0) {
      if (sscanf(buf, "dbid %i", &dbid) != 1 ||
          dbid < 0 ||
          dbid >= MAX_SUBSTREAMS) {
        char *msg = "-1 Invalid dbid\n";
        fwrite(msg, strlen(msg), 1, fp);

        goto done;
      }
      if (rs != NULL && rs->data->n_data > 0) {
        add_enqueue(rs, NULL);
        rs->data->n_data = 0;
      }
    } else if (memcmp(buf, "put", 3) == 0) {
      int streamid, timestamp, sequence, converted, idx;
      double value, min = LLONG_MIN, max = LLONG_MAX;
      if (rs == NULL)
        rs = _rpc_alloc_rs(SMALL_POINTS);
      if (!rs)
        goto done;

      reading__init(rs->data->data[rs->data->n_data]);
      converted = sscanf(buf, "put %i %i %i %lf %lf %lf", 
                         streamid, timestamp, sequence, 
                         value, min, max);
      if (converted < 3) {
        char *msg = "-2 invalid argument\n";
        fwrite(msg, strlen(msg), 1, fp);
        goto done;
      } 

      if (rs->substream != dbid ||
            substream.streamid != streamid) {
        add_enqueue(rs, NULL);
        cmd.args.add.n = 0;
      }
      cmd->substream = dbid;
      cmd->streamid = streamid;
          
      idx = rs->data->n_data++;
      rs->data->data[rs->data->n_data]->timestamp = timestamp;
      rs->data->data[rs->data->n_data]->seqno = sequence;
      rs->data->data[rs->data->n_data]->value = value;
      rs->data->data[rs->data->n_data]->min = min;
      rs->data->data[rs->data->n_data]->max = max;
      if (sequence != 0)
        rs->data->data[rs->data->n_data]->has_seqno = 1;
      if (converted == 6) {
        rs->data->data[rs->data->n_data]->has_min = 1;
        rs->data->data[rs->data->n_data]->has_max = 1;
      }

      if (rs->data->n_data == SMALL_POINTS) {
        add_enqueue(rs, NULL);
        rs->data->n_data = 0;
      }
      INCR_STAT(adds);

    } else if (memcmp(buf, "get", 3) == 0) {
      int i, len;
      unsigned long long streamid, start, end;
      struct ipc_reply *r = malloc(sizeof(struct ipc_reply) + 
                                   (sizeof(struct point) * 
                                    (MAXRECS + MAXBUCKETRECS) ));
      if (!r) {
        char *msg = "-5 no query buffer\n";
        fwrite(msg, strlen(msg), 1, fp);
        break;
      }

      if (sscanf(buf, "get %llu %llu %llu", &streamid, &start, &end) != 3) {
        char *msg = "-3 invalid get\n";
        fwrite(msg, strlen(msg), 1, fp);
        free(r);
        break;
      }

      /* add any pending data before reusing the buffer */
      if (rs && rs->data->n_data > 0) {
        add_enqueue(rs, NULL);
      }

      cmd.command = COMMAND_QUERY;
      cmd.dbid = dbid;
      cmd.streamid = streamid;
      cmd.args.query.starttime = start;
      cmd.args.query.endtime = end;

      //query(dbs[dbid].dbp, &cmd, r);

      if (r->reply != REPLY_OK) {
        char *msg = "-4 query failed\n";
        fwrite(msg, strlen(msg), 1, fp);
        free(r);
        break;
      }

      len = snprintf(buf, sizeof(buf), "%i\n", r->data.query.nrecs);
      fwrite(buf, len, 1, fp);
      
      for (i = 0; i < r->data.query.nrecs; i++) {
        double bottom = LLONG_MIN + 1, top = LLONG_MAX - 1;
        len = snprintf(buf, sizeof(buf), "%i %i %f",
                       r->data.query.pts[i].timestamp,
                       r->data.query.pts[i].reading_sequence,
                       r->data.query.pts[i].reading);
        if (r->data.query.pts[i].min > bottom ||
            r->data.query.pts[i].max < top) {
          len += snprintf(buf + len, sizeof(buf) - len, " %f %f",
                          r->data.query.pts[i].min,
                          r->data.query.pts[i].max);
        }
        buf[len++] = '\n';
        fwrite(buf, len, 1, fp);
      }

      INCR_STAT(queries);
      free(r);
    } else if (memcmp(buf, "binmode", 7) == 0) {
      process_pbuf(req);
      goto done;
    }
  }
  
  if (cmd.command == COMMAND_ADD &&
      cmd.args.add.n > 0) {
    // add_enqueue(&cmd);
  }
#endif
  
 done:
  debug("closing socket\n");
  INCR_STAT(disconnects);
  if (fp)
    fclose(fp);
  else
    close(req->sock);

  free(request);
  WORKER_REMOVE;
  return NULL;
}

int main(int argc, char **argv) {
  struct timeval last, now, delta;
  int yes;
  pthread_t **threads;

  sem_init(&worker_count_sem, 0, MAXCONCURRENCY);

  log_init();

  default_config(&conf);
  if (optparse(argc, argv, &conf) < 0)
    exit(1);

  stats_init(conf.port);

  log_setlevel(conf.loglevel);

  drop_priv();

  // open the database
  db_open(&conf);
  
  signal_setup();
  gettimeofday(&last, NULL);

  int sock = socket(AF_INET6, SOCK_STREAM, 0);
  struct sockaddr_in6 addr = {
    .sin6_family = AF_INET6,
    .sin6_addr = IN6ADDR_ANY_INIT,
    .sin6_port = htons(conf.port),
  };
  

  if (sock < 0) {
    log_fatal_perror("socket");
    goto close;
  }

  if (bind(sock, (struct sockaddr *)&addr, sizeof(struct sockaddr_in6)) < 0) {
    log_fatal_perror("bind");
    goto close;
  }
  info("listening on port %i\n", conf.port);

  yes = 1;
  if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
    log_fatal_perror("setsockopt: SO_REUSEADDR");
    goto close;
  }

  now.tv_sec = 0;
  now.tv_usec = 1e5;  
  if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &now, sizeof(now)) < 0) {
    log_fatal_perror("setsockopt: SO_RCVTIMEO");
    goto close;
  }

  if (listen(sock, 4096) < 0) {
    log_fatal_perror("listen");
    goto close;
  }

  threads = start_threads(&conf);

  while (!do_shutdown) {
    char addr_buf[256];
    struct sockaddr_in6 remote;
    int client, rc;
    socklen_t addrlen = sizeof(struct sockaddr_in6);
    struct sock_request *req;
    pthread_t thread;
    fd_set fs;

    FD_ZERO(&fs);
    FD_SET(sock, &fs);
    now.tv_sec = 0;
    now.tv_usec = 1e5;

    /* use select to poll for new connections, since OSX doesn't cause
       accept(2) to honor the RCVTIMEO we set earlier. */
    if ((rc = select(sock+1, &fs, NULL, NULL, &now)) == 1 && FD_ISSET(sock, &fs)) {
      client = accept(sock, (struct sockaddr *)&remote, &addrlen);
      if (client < 0) {
        if (errno != EAGAIN && errno != EINTR) {
          log_fatal_perror("accept");
        }
        goto do_stats;
        continue;
      }
    } else {
      goto do_stats;
    }
    
    inet_ntop(AF_INET6, &remote.sin6_addr, addr_buf, sizeof(addr_buf));
    debug("Accepted client connection from %s\n", addr_buf);
    INCR_STAT(connects);

    req = malloc(sizeof(struct sock_request));
    if (!req) {
      warn("could not allocate request buffer for client\n");
      close(client);
      continue;
    }

    req->sock = client;

    WORKER_ADD;
    if ((rc = pthread_create(&thread, NULL, process_request, req)) != 0) {
      WORKER_REMOVE;
      close(client);
      free(req);
      warn("could not start new thread for client: [%i] %s\n",
           rc, strerror(rc));
      continue;
    };

    /* this doesn't return any errors we care about */
    pthread_detach(thread);

  do_stats:
    gettimeofday(&now, NULL);
    timeval_subtract(&delta, &now, &last);
    if (delta.tv_sec > 0) {
      int current_workers;
      pthread_mutex_lock(&worker_lock);
      current_workers = worker_count;
      pthread_mutex_unlock(&worker_lock);

      pthread_mutex_lock(&stats_lock);
      float tps = stats.queries + stats.adds;
      tps /= ((float)delta.tv_sec) + (((float)delta.tv_usec) / 1e6);
      info("%li.%06lis: %0.2ftps gets: %i puts: %i put_fails: %i "
           "clients: %i connects: %i disconnects: %i \n",
           delta.tv_sec, delta.tv_usec, tps,
           stats.queries, stats.adds, stats.failed_adds, 
           current_workers, stats.connects, stats.disconnects);
      stats_report(&stats, &now);
      memset(&stats, 0, sizeof(stats));
      pthread_mutex_unlock(&stats_lock);
      gettimeofday(&last, NULL);
    }

  }

  /* don't accept new connections */
  close(sock);

  /* this waits for outstanding client threads to exit */
  WORKER_WAIT;

  /* wait to flush hashtable data and sync */
  info("clients exited, waiting on commit...\n");
  stop_threads(threads);
  info("commit thread exited; closing databases\n");

 close:
  db_close();
  stats_close();
  return 0;
}
