/*
 * Module for computing a covering set of all dirty regions.
 *
 * the main process (reading-server) append to a log of dirty regions.
 * This has the form of a (streamid, starttime, endtime) for each
 * write.  It's relatively inefficient to naively run the sketch
 * updater on all of these tuples, because (a) you have to fetch at
 * least +/- 1 hour of data around each write, and (b) most writes are
 * close together in time.  This file has routines to converting that
 * into a set of intervals which are more than OVERLAP_SLACK apart.
 *
 * The algorithm is basically from 
 *
 *  http://www.geeksforgeeks.org/merging-intervals/
 *
 * ported, to C, and with the addition of the slack parameter.  It's
 * runtime is whatever qsort provides on your platform, and then
 * linear in the input + output size.
 */
/*
 * @author Stephen Dawson-Haggerty <steve@buildingrobotics.com>
 */

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

#include "intervals.h"

/* merge intervals that are less than this apart, even though they
   don't overlap */
#define OVERLAP_SLACK 3600

struct stack {
  struct interval *stack;
  int tail;
  int size;
};

void stack_init(struct stack *s, int n) {
  s->stack = malloc(n * sizeof(struct interval));
  s->tail = 0;
  s->size = n;
}

void stack_push(struct stack *s, struct interval ival) {
  if (s->tail == s->size) {
    s->size *= 2;
    s->stack = realloc(s->stack, s->size * sizeof(struct interval));
  }
  s->stack[s->tail++] = ival;
}

int stack_pop(struct stack *s, struct interval *out) {
  if (s->tail > 0) {
    *out = s->stack[--s->tail];
    return 1;
  } else {
    return 0;
  }
};

struct interval *stack_top(struct stack *s) {
  return &s->stack[s->tail - 1];
}

int cmp_interval(const void *a, const void *b) {
  const struct interval *ai = a, *bi = b;

  if (ai->stream_id != bi->stream_id) {
    return ai->stream_id - bi->stream_id;
  } else {
    if (ai->start != bi->start) {
      return ai->start - bi->start;
    } else {
      return ai->end - bi->end;
    }
  }
};

/* load a dirty region file into memory and sort it. */
struct interval *parse_file(const char *filename, int *n) {
  FILE *fp = fopen(filename, "r");
  int cur_size = 128, cur_idx = 0;
  struct interval *rv;

  if (!fp) {
    return NULL;
  }

  rv = malloc(128 * sizeof(struct interval));

  while (fscanf(fp, "%u\t%u\t%u\n", 
                &rv[cur_idx].stream_id, 
                &rv[cur_idx].start,
                &rv[cur_idx].end) == 3) {
    cur_idx ++;
    if (cur_idx == cur_size) {
      cur_size *= 2;
      rv = realloc(rv, cur_size * sizeof(struct interval));
    }
  }
  *n = cur_idx;

  qsort(rv, cur_idx, sizeof(struct interval), cmp_interval);

  return rv;
}

/* take a sorted list of intervals

   return a list of intervals which are further than OVERLAP_SLACK
   apart, and cover all input intervals
*/
struct interval *merge_intervals(const struct interval *input, int n, int *out_n) {
  /* this is as big as the output can be if it's all unique */
  struct interval *output = malloc(n * sizeof(struct interval));
  int current_output = 0, i;
  struct stack stk;

  stack_init(&stk, 16);

  for (i = 0; i <= n; i++) {
    struct interval *top;
    struct interval cur = input[i];
    cur.end += OVERLAP_SLACK;

    /* if we're into a new stream, output and restart */
    if (i == 0 || i == n || input[i-1].stream_id != input[i].stream_id) {
      /* add the resulting intervals to the output */
      while (stack_pop(&stk, &output[current_output]) != 0) {
        current_output ++;
      }
      if (i == n) {
        break;
      } else {
        /* restart on the next stream */
        stack_push(&stk, cur);
      }
    }

    top = stack_top(&stk);
    if (top->end + 1 < cur.start) {
      stack_push(&stk, cur);
    } else if (top->end < cur.end) {
      top->end = cur.end;
    }
  }
  *out_n = current_output;
  return output;
};
