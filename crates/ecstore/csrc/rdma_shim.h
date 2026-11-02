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

// Thin, non-inline wrappers over libibverbs and librdmacm.
//
// libibverbs exposes ibv_post_send/ibv_post_recv/ibv_poll_cq/ibv_req_notify_cq
// as static inline functions, so Rust cannot link them directly. This shim
// keeps every verbs struct layout on the C side and exposes opaque handles plus
// plain integers to Rust. All functions return 0 or a positive count on success
// and a negative errno value on failure.

#ifndef RUSTFS_RDMA_SHIM_H
#define RUSTFS_RDMA_SHIM_H

#include <stddef.h>
#include <stdint.h>

struct rfs_rdma_listener;
struct rfs_rdma_conn;
struct rfs_rdma_mr;
struct rfs_rdma_device;

struct rfs_rdma_qp_limits {
    uint32_t send_depth;
    uint32_t recv_depth;
    uint32_t cq_depth;
};

struct rfs_rdma_wc {
    uint64_t wr_id;
    uint32_t status;
    uint32_t opcode;
    uint32_t byte_len;
    uint32_t vendor_err;
};

enum rfs_rdma_cm_status {
    RFS_RDMA_CM_NONE = 0,
    RFS_RDMA_CM_ESTABLISHED = 1,
    RFS_RDMA_CM_DISCONNECTED = 2,
    RFS_RDMA_CM_REJECTED = 3,
    RFS_RDMA_CM_UNREACHABLE = 4,
    RFS_RDMA_CM_ERROR = 5,
};

enum rfs_rdma_wc_opcode {
    RFS_RDMA_WC_SEND = 0,
    RFS_RDMA_WC_RDMA_WRITE = 1,
    RFS_RDMA_WC_RECV = 2,
    RFS_RDMA_WC_OTHER = 3,
};

int rfs_rdma_listener_create(const char *ip, uint16_t port, int backlog, struct rfs_rdma_listener **out);
int rfs_rdma_listener_fd(const struct rfs_rdma_listener *listener);
// Returns 1 and sets *out when a connection request produced a pending
// connection, 0 when no request is available, and a negative errno on failure.
int rfs_rdma_listener_accept(struct rfs_rdma_listener *listener, const struct rfs_rdma_qp_limits *limits,
                             struct rfs_rdma_conn **out);
void rfs_rdma_listener_destroy(struct rfs_rdma_listener *listener);

// Resolves the address and route synchronously (bounded by timeout_ms) and
// creates the queue pair. The connection is established by rfs_rdma_conn_connect.
int rfs_rdma_client_create(const char *ip, uint16_t port, int timeout_ms, const struct rfs_rdma_qp_limits *limits,
                           struct rfs_rdma_conn **out);
int rfs_rdma_conn_connect(struct rfs_rdma_conn *conn);
int rfs_rdma_conn_accept(struct rfs_rdma_conn *conn);
int rfs_rdma_conn_cm_fd(const struct rfs_rdma_conn *conn);
// Consumes one connection-manager event; returns an rfs_rdma_cm_status or a negative errno.
int rfs_rdma_conn_poll_cm(struct rfs_rdma_conn *conn);
int rfs_rdma_conn_comp_fd(const struct rfs_rdma_conn *conn);
// Drains completion-channel events, re-arms notification, and polls the CQ.
int rfs_rdma_conn_poll_completions(struct rfs_rdma_conn *conn, struct rfs_rdma_wc *out, int max);
struct rfs_rdma_device *rfs_rdma_conn_device(const struct rfs_rdma_conn *conn);
int rfs_rdma_conn_post_recv(struct rfs_rdma_conn *conn, uint32_t lkey, uint64_t addr, uint32_t len, uint64_t wr_id);
int rfs_rdma_conn_post_send(struct rfs_rdma_conn *conn, uint32_t lkey, uint64_t addr, uint32_t len, uint64_t wr_id);
int rfs_rdma_conn_post_write(struct rfs_rdma_conn *conn, uint32_t lkey, uint64_t addr, uint32_t len,
                             uint64_t remote_addr, uint32_t rkey, uint64_t wr_id);
int rfs_rdma_conn_peer_addr(const struct rfs_rdma_conn *conn, char *buf, size_t buf_len);
// Moves the queue pair to the error state so no further DMA touches any buffer; idempotent.
void rfs_rdma_conn_abort(struct rfs_rdma_conn *conn);
void rfs_rdma_conn_close(struct rfs_rdma_conn *conn);

int rfs_rdma_device_reg_mr(struct rfs_rdma_device *device, void *addr, size_t len, struct rfs_rdma_mr **out);
uint32_t rfs_rdma_mr_lkey(const struct rfs_rdma_mr *mr);
uint32_t rfs_rdma_mr_rkey(const struct rfs_rdma_mr *mr);
void rfs_rdma_mr_dereg(struct rfs_rdma_mr *mr);

const char *rfs_rdma_wc_status_str(uint32_t status);

#endif
