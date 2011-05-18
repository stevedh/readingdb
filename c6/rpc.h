#ifndef _RPC_H_
#define _RPC_H_

#include "pbuf/rdb.pb-c.h"

void _rpc_copy_records(struct point *dest, Reading **src, int n);
void _rpc_copy_reading(Reading *dest, struct point *src);
ReadingSet *_rpc_alloc_rs(int n);
void _rpc_free_rs(ReadingSet *rs);
void rpc_send_reply(struct sock_request *request, 
                    Response *response);

#endif
