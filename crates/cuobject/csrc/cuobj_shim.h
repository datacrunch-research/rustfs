// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// C wrapper over the C++ cuObject server library (libcuobjserver).
// Every function returns 0 or a positive byte count on success and a negative
// errno on failure; exceptions never cross this boundary.

#ifndef RUSTFS_CUOBJ_SHIM_H
#define RUSTFS_CUOBJ_SHIM_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

struct rfs_cuobj_server;
struct rfs_cuobj_buffer;

// Creates a cuObject server bound to the RDMA device that owns `ip`.
int rfs_cuobj_server_create(const char *ip, uint16_t port, int num_dcis, unsigned cq_depth, int max_sge,
                            struct rfs_cuobj_server **out);
void rfs_cuobj_server_destroy(struct rfs_cuobj_server *server);

// Allocates a page-aligned host buffer suitable for registration.
void *rfs_cuobj_alloc_host(struct rfs_cuobj_server *server, size_t size);
int rfs_cuobj_register(struct rfs_cuobj_server *server, void *ptr, size_t size, struct rfs_cuobj_buffer **out);
void rfs_cuobj_deregister(struct rfs_cuobj_server *server, struct rfs_cuobj_buffer *buffer);

// One entry of a scatter-gather list over a registered buffer.
struct rfs_cuobj_sg_entry {
    uint64_t offset;
    size_t size;
};
// Builds a transfer handle over `count` (offset, size) ranges of the registered
// `base` buffer (the library supports at most its `max_sge` entries, 10 by
// default); one get/put then moves all ranges in order as one RDMA operation.
// Release it with rfs_cuobj_sg_buffer_put, never with rfs_cuobj_deregister.
int rfs_cuobj_sg_buffer(struct rfs_cuobj_server *server, struct rfs_cuobj_buffer *base,
                        const struct rfs_cuobj_sg_entry *entries, size_t count, struct rfs_cuobj_buffer **out);
void rfs_cuobj_sg_buffer_put(struct rfs_cuobj_server *server, struct rfs_cuobj_buffer *sg);
int rfs_cuobj_channel_alloc(struct rfs_cuobj_server *server, uint16_t *out);
void rfs_cuobj_channel_free(struct rfs_cuobj_server *server, uint16_t channel);

// One completed asynchronous operation: the handle given at submission and
// the operation's result (bytes moved, or a negative errno).
struct rfs_cuobj_completion {
    void *handle;
    ssize_t status;
};
// Submits a GET (an RDMA write into the client's memory) on `channel` and
// returns at once; `handle` (non-NULL) comes back through
// rfs_cuobj_process_completions on the channel's completion descriptor.
// Returns 0 on submission, or a negative errno.
ssize_t rfs_cuobj_get_async(struct rfs_cuobj_server *server, const char *key, struct rfs_cuobj_buffer *buffer,
                            uint64_t local_offset, uint64_t remote_addr, size_t size, const char *descriptor,
                            uint16_t channel, void *handle);
// The channel's pollable completion descriptor (owned by the library, never
// closed by the caller), or -1 when the library offers none.
int rfs_cuobj_channel_completion_fd(struct rfs_cuobj_server *server, uint16_t channel);
// Drains the completions behind `fd` into `out`; returns the count filled (call
// again before waiting when it equals `max`), or a negative errno.
int rfs_cuobj_process_completions(struct rfs_cuobj_server *server, int fd, struct rfs_cuobj_completion *out,
                                  int max);

// RDMA-writes `size` bytes from `buffer + local_offset` into the client memory
// described by `descriptor` starting at `remote_addr`. Blocks until the write
// completed; returns the byte count or a negative errno (-EPROTO for a
// malformed or mismatched descriptor).
ssize_t rfs_cuobj_get(struct rfs_cuobj_server *server, const char *key, struct rfs_cuobj_buffer *buffer,
                      uint64_t local_offset, uint64_t remote_addr, size_t size, const char *descriptor,
                      uint16_t channel, int *verbs_status);

// RDMA-reads `size` bytes from client memory into `buffer + local_offset`.
ssize_t rfs_cuobj_put(struct rfs_cuobj_server *server, const char *key, struct rfs_cuobj_buffer *buffer,
                      uint64_t local_offset, uint64_t remote_addr, size_t size, const char *descriptor,
                      uint16_t channel, int *verbs_status);

// Library version as packed major*1000+minor.
int rfs_cuobj_version(void);

#ifdef __cplusplus
}
#endif

#endif
