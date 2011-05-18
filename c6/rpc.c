
#include <stdlib.h>
#include <limits.h>
#include <arpa/inet.h>

#include "readingdb.h"
#include "pbuf/rdb.pb-c.h"

void _rpc_copy_records(struct point *dest, Reading **src, int n) {
  int i;
  for (i = 0; i < n; i++) {
    dest[i].timestamp = src[i]->timestamp;
    dest[i].reading = src[i]->value;

    if (src[i]->has_seqno)
      dest[i].reading_sequence = src[i]->seqno;
    else
      dest[i].reading_sequence = 0;

    if (src[i]->has_max)
      dest[i].max = src[i]->max;
    else
      dest[i].max = LLONG_MAX;

    if (src[i]->has_min)
      dest[i].min = src[i]->min;
    else
      dest[i].min = LLONG_MIN;
  }
}

void _rpc_copy_reading(Reading *dest, struct point *src) {
  double bottom = LLONG_MIN + 1, top = LLONG_MAX - 1;
  reading__init(dest);

  dest->timestamp = src->timestamp;
  dest->value = src->reading;

  /* optional fields */
  if (src->reading_sequence != 0)
    dest->has_seqno = 1;
  if (src->min > bottom)
    dest->has_min = 1;
  if (src->max < top)
    dest->has_max = 1;
  
  dest->seqno = src->reading_sequence;
  dest->min = src->min;
  dest->max = src->max;
}

ReadingSet *_rpc_alloc_rs(int n) {
  int i;
  ReadingSet *points;
  Reading *points_data;
  Reading **points_vec;

  /* allocate a copy of the input reading set */
  points = malloc(sizeof(ReadingSet));
  points_vec = malloc(sizeof(Reading *) * n);
  points_data = malloc(sizeof(Reading) * n);
  if (!points || !points_vec || !points_data) {
    if (points) free(points);
    if (points_vec) free(points_vec);
    if (points_data) free(points_data);
    return NULL;
  }

  reading_set__init(points);
  points->n_data = 0;
  points->data = points_vec;
  for (i = 0; i < n; i++)
    points_vec[i] = &points_data[i];
  
  return points;
}

void _rpc_free_rs(ReadingSet *rs) {
  free(rs->data[0]);
  free(rs->data);
  free(rs);
}

int rpc_send_reply(struct sock_request *request, 
                    Response *response) {
  void *reply;
  int reply_len, i_alloced = 0;
  if (response->data == NULL) {
    response->data = _rpc_alloc_rs(1);
    i_alloced = 1;
  }

  reply_len = response__get_packed_size(response);
  reply = malloc(reply_len);
  if (reply) {
    struct pbuf_header h;
    h.message_type = htonl(MESSAGE_TYPE__RESPONSE);
    h.body_length = htonl(reply_len);
    response__pack(response, reply);
    if (fwrite(&h, sizeof(h), 1, request->sock_fp) <= 0)
      goto fail;
    if (fwrite(reply, reply_len, 1, request->sock_fp) <= 0)
      goto fail;
    // info("wrote %i bytes\n", sizeof(h) + reply_len);
    fflush(request->sock_fp);
    free(reply);
    if (i_alloced)
      _rpc_free_rs(response->data);
    return 0;
  }
 fail:
  if (i_alloced)
    _rpc_free_rs(response->data);
  return -1;
}

