
#include <stdlib.h>
#include <check.h>
#include "../src/hashtable.h"

unsigned int int_hashfunction(void *h) {
  unsigned int *v = h;
  return *v;
}
unsigned int int_eq_fn(void *k1, void *k2) {
  unsigned int *i1 = k1;
  unsigned int *i2 = k2;
  return (*i1 == *i2);
}

START_TEST(test_hashtable_create)
{
  int *k, *v;
  struct hashtable *h = create_hashtable(12, int_hashfunction, int_eq_fn);
  fail_unless(hashtable_count(h) == 0, 
              "Table is not empty on creation");
  hashtable_destroy(h, 1);
}
END_TEST

START_TEST(test_hashtable_insert_count)
{
  int i, *k, *v;
  struct hashtable *h = create_hashtable(12, int_hashfunction, int_eq_fn);
  for (i = 0; i < 10; i++) {
    k = malloc(sizeof(int));
    v = malloc(sizeof(int));
    *k = i;
    fail_unless(hashtable_count(h) == i,
                "Table count is not %i", i);
    hashtable_insert(h, k, v);
  }
  hashtable_destroy(h, 1);
}
END_TEST

START_TEST(test_hashtable_next_empty)
{
  void *k, *v;
  struct hashtable *h = create_hashtable(12, int_hashfunction, int_eq_fn);
  k = NULL;
  v = hashtable_next(h, &k);
  fail_unless(v == NULL,
              "hashtable_next must return NULL on an empty table");
  hashtable_destroy(h, 1);
}
END_TEST

START_TEST(test_hashtable_next_one) 
{
  int *k, *v, *qk, *qv;
  struct hashtable *h = create_hashtable(12, int_hashfunction, int_eq_fn);
  k = malloc(sizeof(int));
  v = malloc(sizeof(int));
  *k = 12;
  hashtable_insert(h, k, v);
  qk = NULL;
  qv = hashtable_next(h, &qk);
  fail_unless(qk == k,
              "hashtable_next did not return only key");
  fail_unless(qv == v,
              "hashtable_next did not return correct value");
  qv = hashtable_next(h, &qk);
  fail_unless(qv == NULL,
              "hashtable_next did not empty");
  hashtable_destroy(h, 1);
}
END_TEST

/* check that we get all the keys back, exactly once */
START_TEST(test_hashtable_next_several) 
{
  int i, *k, *v, *qk, *qv;
  int read_vals = 0, read_n = 0;
  struct hashtable *h = create_hashtable(12, int_hashfunction, int_eq_fn);
  for (i = 0; i < 16; i++) {
    k = malloc(sizeof(int));
    v = malloc(sizeof(int));
    *k = i;
    *v = i;
    hashtable_insert(h, k, v);
  }
  fail_unless(hashtable_count(h) == 16,
              "Did not insert all 16 keys");
  qk = NULL;
  qv = hashtable_next(h, &qk);
  while (qv != NULL) {
    fail_unless(*qk == *qv,
                "next did not return matching keys: %i %i", *qk, *qv);
    read_vals |= 1 << *qk;
    read_n ++;
    if (read_n > 30)
      break;
    qv = hashtable_next(h, &qk);
  }
  fail_unless(read_n == 16,
              "hashtable_next did not return exactly 16 values (%i)", read_n);
  fail_unless(read_vals == 0xffff,
              "hashtable_next did not return all 16 values");
  hashtable_destroy(h, 1);
}
END_TEST

Suite *
hashtable_suite(void) {
  Suite *s = suite_create("Hashtable");
  TCase *tc_core = tcase_create("Core");
  TCase *tc_next = tcase_create("Next");

  /* core tests */
  tcase_add_test(tc_core, test_hashtable_create);
  tcase_add_test(tc_core, test_hashtable_insert_count);
  suite_add_tcase(s, tc_core);

  /* next tests */
  tcase_add_test(tc_next, test_hashtable_next_empty);
  tcase_add_test(tc_next, test_hashtable_next_one);
  tcase_add_test(tc_next, test_hashtable_next_several);
  suite_add_tcase(s, tc_next);

  return s;
}

int main(void) 
{
  int number_failed;
  Suite *s = hashtable_suite();
  SRunner *sr = srunner_create(s);
  srunner_run_all(sr, CK_NORMAL);
  number_failed = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
