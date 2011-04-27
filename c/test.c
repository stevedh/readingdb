
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#define FREELIST(type, name) \
 static type *freelist_ ## name = NULL; \
 static type *freelist_ ## name ## _tmp;

#define FREELIST_GET(type, name) \
 ((freelist_ ## name) == NULL ? ((type *)(malloc(sizeof(type)))) : \
 ((freelist_ ## name ## _tmp) = freelist_ ## name, \
  freelist_ ## name = *((type **)(freelist_ ## name)), \
  freelist_ ## name ## _tmp))

#define FREELIST_PUT(type, name, val)  \
 {\
   *(type **)(val) = freelist_ ## name; \
   freelist_ ## name = val;             \
}

FREELIST(int, mylist);

int main() {
  int *x, *y;
  double val = LLONG_MAX;

  printf("%f\n", val);
  return 0;

  x = FREELIST_GET(int, mylist);
  y = FREELIST_GET(int, mylist);
  printf("%p %p\n", x, y);

  FREELIST_PUT(int, mylist, x);
  x = FREELIST_GET(int, mylist);
  printf("%p\n", x);

  FREELIST_PUT(int, mylist, x);
  FREELIST_PUT(int, mylist, y);
  x = FREELIST_GET(int, mylist);
  y = FREELIST_GET(int, mylist);
  printf("%p %p\n", x, y);
}
