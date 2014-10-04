struct interval {
  uint32_t stream_id;
  uint32_t start;
  uint32_t end;
};

struct interval *parse_file(const char *filename, int *n);
struct interval *merge_intervals(const struct interval *input, int n, int *out_n);
