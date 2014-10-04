#ifndef SKETCH_H
#define SKETCH_H

#include <stdint.h>

#include "pbuf/rdb.pb-c.h"

/* sketch definitions.
 *
 * These must be sorted smallest to largest, and all window sizes must
 * divide the largest window size.  The results will be placed into
 * the substream corresponding to the sketch definition index in this
 * array.
 */
struct sketch {
  /* period of sketch computation, seconds */
  int period;
  /* function that computes the window */
  /* a window function gets passed a readingset, start, and end value */
  /* it should return a new ReadingSet of window values it wants to update */
  int nsubstreams;
} sketches [] = {
  { 300, 4},           /* returns count, mean, min, max */
  { 900, 4},           /* returns count, mean, min, max */
  { 3600, 4},
};

/* these match the sketch type enum in rdb.proto */
char *sketch_names[] = {
  "null",
  "count",
  "mean", 
  "min",
  "max",
  "first",
  "median"
};

#define DIRTY_SKETCH_LOGFILE "dirty_sketches.log"

/* map a sketch to a substream, badly...  */
int get_sketch_substream(Sketch *s) {
  if (!(s->type > 0 && s->type <= SKETCH__SKETCH_TYPE__MAX)) {
    return -1;
  }
  switch (s->window) {
  case 300:
    return ((int)s->type);
  case 900:
    return ((int)s->type) + 4;
  case 3600:
    return ((int)s->type) + 8;
  default:
    return -1;
  }
};

#endif
