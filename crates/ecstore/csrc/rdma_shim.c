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

#define _GNU_SOURCE

#include "rdma_shim.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

// One protection domain per device context. Memory regions registered in a
// protection domain are usable by every queue pair created in it, so the
// shared buffer pool registers once per device instead of once per connection.
struct rfs_rdma_device {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct rfs_rdma_device *next;
};

static pthread_mutex_t g_device_lock = PTHREAD_MUTEX_INITIALIZER;
static struct rfs_rdma_device *g_devices;

struct rfs_rdma_listener {
    struct rdma_event_channel *channel;
    struct rdma_cm_id *id;
};

struct rfs_rdma_conn {
    struct rdma_event_channel *channel;
    struct rdma_cm_id *id;
    struct rfs_rdma_device *device;
    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *cq;
    int connected;
};

struct rfs_rdma_mr {
    struct ibv_mr *mr;
};

static int neg_errno(void) {
    return errno ? -errno : -EIO;
}

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0) {
        return neg_errno();
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return neg_errno();
    }
    return 0;
}

static int parse_sockaddr(const char *ip, uint16_t port, struct sockaddr_storage *out) {
    memset(out, 0, sizeof(*out));
    struct sockaddr_in *v4 = (struct sockaddr_in *)out;
    if (inet_pton(AF_INET, ip, &v4->sin_addr) == 1) {
        v4->sin_family = AF_INET;
        v4->sin_port = htons(port);
        return 0;
    }
    struct sockaddr_in6 *v6 = (struct sockaddr_in6 *)out;
    if (inet_pton(AF_INET6, ip, &v6->sin6_addr) == 1) {
        v6->sin6_family = AF_INET6;
        v6->sin6_port = htons(port);
        return 0;
    }
    return -EINVAL;
}

static struct rfs_rdma_device *device_for(struct ibv_context *ctx) {
    pthread_mutex_lock(&g_device_lock);
    struct rfs_rdma_device *device = g_devices;
    while (device != NULL) {
        if (device->ctx == ctx) {
            pthread_mutex_unlock(&g_device_lock);
            return device;
        }
        device = device->next;
    }
    device = calloc(1, sizeof(*device));
    if (device == NULL) {
        pthread_mutex_unlock(&g_device_lock);
        errno = ENOMEM;
        return NULL;
    }
    device->ctx = ctx;
    device->pd = ibv_alloc_pd(ctx);
    if (device->pd == NULL) {
        int saved = errno;
        free(device);
        pthread_mutex_unlock(&g_device_lock);
        errno = saved ? saved : EIO;
        return NULL;
    }
    device->next = g_devices;
    g_devices = device;
    pthread_mutex_unlock(&g_device_lock);
    return device;
}

static void conn_free(struct rfs_rdma_conn *conn) {
    if (conn == NULL) {
        return;
    }
    if (conn->id != NULL && conn->id->qp != NULL) {
        rdma_destroy_qp(conn->id);
    }
    if (conn->cq != NULL) {
        ibv_destroy_cq(conn->cq);
    }
    if (conn->comp_channel != NULL) {
        ibv_destroy_comp_channel(conn->comp_channel);
    }
    if (conn->id != NULL) {
        rdma_destroy_id(conn->id);
    }
    if (conn->channel != NULL) {
        rdma_destroy_event_channel(conn->channel);
    }
    free(conn);
}

// Creates the completion machinery and queue pair for a resolved or requested id.
static int conn_new(struct rdma_cm_id *id, struct rdma_event_channel *channel, const struct rfs_rdma_qp_limits *limits,
                    struct rfs_rdma_conn **out) {
    struct rfs_rdma_conn *conn = calloc(1, sizeof(*conn));
    if (conn == NULL) {
        return -ENOMEM;
    }
    conn->id = id;
    conn->channel = channel;
    conn->device = device_for(id->verbs);
    if (conn->device == NULL) {
        int err = neg_errno();
        free(conn);
        return err;
    }
    conn->comp_channel = ibv_create_comp_channel(id->verbs);
    if (conn->comp_channel == NULL) {
        int err = neg_errno();
        free(conn);
        return err;
    }
    int err = set_nonblocking(conn->comp_channel->fd);
    if (err != 0) {
        ibv_destroy_comp_channel(conn->comp_channel);
        free(conn);
        return err;
    }
    conn->cq = ibv_create_cq(id->verbs, (int)limits->cq_depth, NULL, conn->comp_channel, 0);
    if (conn->cq == NULL) {
        err = neg_errno();
        ibv_destroy_comp_channel(conn->comp_channel);
        free(conn);
        return err;
    }
    if (ibv_req_notify_cq(conn->cq, 0) != 0) {
        err = neg_errno();
        ibv_destroy_cq(conn->cq);
        ibv_destroy_comp_channel(conn->comp_channel);
        free(conn);
        return err;
    }
    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.send_cq = conn->cq;
    attr.recv_cq = conn->cq;
    attr.qp_type = IBV_QPT_RC;
    attr.cap.max_send_wr = limits->send_depth;
    attr.cap.max_recv_wr = limits->recv_depth;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.sq_sig_all = 1;
    if (rdma_create_qp(id, conn->device->pd, &attr) != 0) {
        err = neg_errno();
        ibv_destroy_cq(conn->cq);
        ibv_destroy_comp_channel(conn->comp_channel);
        free(conn);
        return err;
    }
    *out = conn;
    return 0;
}

int rfs_rdma_listener_create(const char *ip, uint16_t port, int backlog, struct rfs_rdma_listener **out) {
    struct sockaddr_storage addr;
    int err = parse_sockaddr(ip, port, &addr);
    if (err != 0) {
        return err;
    }
    struct rfs_rdma_listener *listener = calloc(1, sizeof(*listener));
    if (listener == NULL) {
        return -ENOMEM;
    }
    listener->channel = rdma_create_event_channel();
    if (listener->channel == NULL) {
        err = neg_errno();
        free(listener);
        return err;
    }
    if (rdma_create_id(listener->channel, &listener->id, NULL, RDMA_PS_TCP) != 0) {
        err = neg_errno();
        rdma_destroy_event_channel(listener->channel);
        free(listener);
        return err;
    }
    if (rdma_bind_addr(listener->id, (struct sockaddr *)&addr) != 0 || rdma_listen(listener->id, backlog) != 0) {
        err = neg_errno();
        rfs_rdma_listener_destroy(listener);
        return err;
    }
    err = set_nonblocking(listener->channel->fd);
    if (err != 0) {
        rfs_rdma_listener_destroy(listener);
        return err;
    }
    *out = listener;
    return 0;
}

int rfs_rdma_listener_fd(const struct rfs_rdma_listener *listener) {
    return listener->channel->fd;
}

int rfs_rdma_listener_accept(struct rfs_rdma_listener *listener, const struct rfs_rdma_qp_limits *limits,
                             struct rfs_rdma_conn **out) {
    struct rdma_cm_event *event = NULL;
    if (rdma_get_cm_event(listener->channel, &event) != 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        }
        return neg_errno();
    }
    if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
        // A stale event for a child id that was torn down before it migrated
        // off this channel. The caller must keep draining: returning "no event"
        // here would clear the edge-triggered readiness with requests still queued.
        rdma_ack_cm_event(event);
        return 2;
    }
    struct rdma_cm_id *id = event->id;
    rdma_ack_cm_event(event);

    // Give the connection its own event channel so the listener loop never
    // sees per-connection events and connection teardown never depends on it.
    struct rdma_event_channel *channel = rdma_create_event_channel();
    if (channel == NULL) {
        int err = neg_errno();
        rdma_reject(id, NULL, 0);
        rdma_destroy_id(id);
        return err;
    }
    if (rdma_migrate_id(id, channel) != 0) {
        int err = neg_errno();
        rdma_reject(id, NULL, 0);
        rdma_destroy_id(id);
        rdma_destroy_event_channel(channel);
        return err;
    }
    int err = set_nonblocking(channel->fd);
    if (err != 0) {
        rdma_reject(id, NULL, 0);
        rdma_destroy_id(id);
        rdma_destroy_event_channel(channel);
        return err;
    }
    struct rfs_rdma_conn *conn = NULL;
    err = conn_new(id, channel, limits, &conn);
    if (err != 0) {
        rdma_reject(id, NULL, 0);
        rdma_destroy_id(id);
        rdma_destroy_event_channel(channel);
        return err;
    }
    *out = conn;
    return 1;
}

void rfs_rdma_listener_destroy(struct rfs_rdma_listener *listener) {
    if (listener == NULL) {
        return;
    }
    if (listener->id != NULL) {
        rdma_destroy_id(listener->id);
    }
    if (listener->channel != NULL) {
        rdma_destroy_event_channel(listener->channel);
    }
    free(listener);
}

// Waits for one expected connection-manager event on a blocking channel.
static int wait_cm_event(struct rdma_event_channel *channel, enum rdma_cm_event_type expected, int timeout_ms) {
    struct pollfd pfd;
    pfd.fd = channel->fd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    int ready = poll(&pfd, 1, timeout_ms);
    if (ready < 0) {
        return neg_errno();
    }
    if (ready == 0) {
        return -ETIMEDOUT;
    }
    struct rdma_cm_event *event = NULL;
    if (rdma_get_cm_event(channel, &event) != 0) {
        return neg_errno();
    }
    enum rdma_cm_event_type got = event->event;
    int status = event->status;
    rdma_ack_cm_event(event);
    if (got == expected) {
        return 0;
    }
    switch (got) {
    case RDMA_CM_EVENT_ADDR_ERROR:
    case RDMA_CM_EVENT_ROUTE_ERROR:
        return status == -ETIMEDOUT ? -ETIMEDOUT : -EHOSTUNREACH;
    case RDMA_CM_EVENT_UNREACHABLE:
        return -EHOSTUNREACH;
    case RDMA_CM_EVENT_REJECTED:
        return -ECONNREFUSED;
    default:
        return -EIO;
    }
}

int rfs_rdma_client_create(const char *ip, uint16_t port, int timeout_ms, const struct rfs_rdma_qp_limits *limits,
                           struct rfs_rdma_conn **out) {
    struct sockaddr_storage addr;
    int err = parse_sockaddr(ip, port, &addr);
    if (err != 0) {
        return err;
    }
    struct rdma_event_channel *channel = rdma_create_event_channel();
    if (channel == NULL) {
        return neg_errno();
    }
    struct rdma_cm_id *id = NULL;
    if (rdma_create_id(channel, &id, NULL, RDMA_PS_TCP) != 0) {
        err = neg_errno();
        rdma_destroy_event_channel(channel);
        return err;
    }
    if (rdma_resolve_addr(id, NULL, (struct sockaddr *)&addr, timeout_ms) != 0) {
        err = neg_errno();
        goto fail;
    }
    err = wait_cm_event(channel, RDMA_CM_EVENT_ADDR_RESOLVED, timeout_ms);
    if (err != 0) {
        goto fail;
    }
    if (rdma_resolve_route(id, timeout_ms) != 0) {
        err = neg_errno();
        goto fail;
    }
    err = wait_cm_event(channel, RDMA_CM_EVENT_ROUTE_RESOLVED, timeout_ms);
    if (err != 0) {
        goto fail;
    }
    err = set_nonblocking(channel->fd);
    if (err != 0) {
        goto fail;
    }
    struct rfs_rdma_conn *conn = NULL;
    err = conn_new(id, channel, limits, &conn);
    if (err != 0) {
        goto fail;
    }
    *out = conn;
    return 0;

fail:
    rdma_destroy_id(id);
    rdma_destroy_event_channel(channel);
    return err;
}

static void init_conn_param(struct rdma_conn_param *param) {
    memset(param, 0, sizeof(*param));
    param->responder_resources = 1;
    param->initiator_depth = 1;
    param->retry_count = 7;
    // Receive buffers are always posted; an RNR retry only covers a transient
    // scheduling gap, so never fail a transfer on it.
    param->rnr_retry_count = 7;
}

int rfs_rdma_conn_connect(struct rfs_rdma_conn *conn) {
    struct rdma_conn_param param;
    init_conn_param(&param);
    if (rdma_connect(conn->id, &param) != 0) {
        return neg_errno();
    }
    return 0;
}

int rfs_rdma_conn_accept(struct rfs_rdma_conn *conn) {
    struct rdma_conn_param param;
    init_conn_param(&param);
    if (rdma_accept(conn->id, &param) != 0) {
        return neg_errno();
    }
    return 0;
}

int rfs_rdma_conn_cm_fd(const struct rfs_rdma_conn *conn) {
    return conn->channel->fd;
}

int rfs_rdma_conn_poll_cm(struct rfs_rdma_conn *conn) {
    struct rdma_cm_event *event = NULL;
    if (rdma_get_cm_event(conn->channel, &event) != 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return RFS_RDMA_CM_NONE;
        }
        return neg_errno();
    }
    enum rdma_cm_event_type got = event->event;
    rdma_ack_cm_event(event);
    switch (got) {
    case RDMA_CM_EVENT_ESTABLISHED:
        conn->connected = 1;
        return RFS_RDMA_CM_ESTABLISHED;
    case RDMA_CM_EVENT_DISCONNECTED:
        conn->connected = 0;
        return RFS_RDMA_CM_DISCONNECTED;
    case RDMA_CM_EVENT_REJECTED:
        return RFS_RDMA_CM_REJECTED;
    case RDMA_CM_EVENT_UNREACHABLE:
        return RFS_RDMA_CM_UNREACHABLE;
    case RDMA_CM_EVENT_CONNECT_ERROR:
    case RDMA_CM_EVENT_ADDR_ERROR:
    case RDMA_CM_EVENT_ROUTE_ERROR:
    case RDMA_CM_EVENT_DEVICE_REMOVAL:
        return RFS_RDMA_CM_ERROR;
    default:
        return RFS_RDMA_CM_NONE;
    }
}

int rfs_rdma_conn_comp_fd(const struct rfs_rdma_conn *conn) {
    return conn->comp_channel->fd;
}

static uint32_t map_opcode(enum ibv_wc_opcode opcode) {
    switch (opcode) {
    case IBV_WC_SEND:
        return RFS_RDMA_WC_SEND;
    case IBV_WC_RDMA_WRITE:
        return RFS_RDMA_WC_RDMA_WRITE;
    case IBV_WC_RECV:
    case IBV_WC_RECV_RDMA_WITH_IMM:
        return RFS_RDMA_WC_RECV;
    default:
        return RFS_RDMA_WC_OTHER;
    }
}

static int poll_cq_into(struct ibv_cq *cq, struct rfs_rdma_wc *out, int max) {
    struct ibv_wc wcs[16];
    int total = 0;
    while (total < max) {
        int batch = max - total;
        if (batch > 16) {
            batch = 16;
        }
        int n = ibv_poll_cq(cq, batch, wcs);
        if (n < 0) {
            return -EIO;
        }
        for (int i = 0; i < n; i++) {
            out[total + i].wr_id = wcs[i].wr_id;
            out[total + i].status = (uint32_t)wcs[i].status;
            // Flushed or failed work requests carry an undefined opcode.
            out[total + i].opcode = wcs[i].status == IBV_WC_SUCCESS ? map_opcode(wcs[i].opcode) : RFS_RDMA_WC_OTHER;
            out[total + i].byte_len = wcs[i].byte_len;
            out[total + i].vendor_err = wcs[i].vendor_err;
        }
        total += n;
        if (n < batch) {
            break;
        }
    }
    return total;
}

int rfs_rdma_conn_poll_completions(struct rfs_rdma_conn *conn, struct rfs_rdma_wc *out, int max) {
    if (max <= 0) {
        return -EINVAL;
    }
    int n = poll_cq_into(conn->cq, out, max);
    if (n != 0) {
        return n;
    }
    // Consume every pending completion event, then re-arm notification before
    // the final poll so a completion that lands in between still raises an event.
    unsigned int events = 0;
    for (;;) {
        struct ibv_cq *event_cq = NULL;
        void *event_ctx = NULL;
        if (ibv_get_cq_event(conn->comp_channel, &event_cq, &event_ctx) != 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            if (events != 0) {
                ibv_ack_cq_events(conn->cq, events);
            }
            return neg_errno();
        }
        events++;
    }
    if (events != 0) {
        ibv_ack_cq_events(conn->cq, events);
    }
    if (ibv_req_notify_cq(conn->cq, 0) != 0) {
        return neg_errno();
    }
    return poll_cq_into(conn->cq, out, max);
}

struct rfs_rdma_device *rfs_rdma_conn_device(const struct rfs_rdma_conn *conn) {
    return conn->device;
}

struct rfs_rdma_device *rfs_rdma_listener_device(const struct rfs_rdma_listener *listener) {
    if (listener == NULL || listener->id == NULL || listener->id->verbs == NULL) {
        return NULL;
    }
    return device_for(listener->id->verbs);
}

int rfs_rdma_conn_post_recv(struct rfs_rdma_conn *conn, uint32_t lkey, uint64_t addr, uint32_t len, uint64_t wr_id) {
    struct ibv_sge sge;
    sge.addr = addr;
    sge.length = len;
    sge.lkey = lkey;
    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    struct ibv_recv_wr *bad = NULL;
    int err = ibv_post_recv(conn->id->qp, &wr, &bad);
    return err == 0 ? 0 : -err;
}

int rfs_rdma_conn_post_send(struct rfs_rdma_conn *conn, uint32_t lkey, uint64_t addr, uint32_t len, uint64_t wr_id) {
    struct ibv_sge sge;
    sge.addr = addr;
    sge.length = len;
    sge.lkey = lkey;
    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;
    struct ibv_send_wr *bad = NULL;
    int err = ibv_post_send(conn->id->qp, &wr, &bad);
    return err == 0 ? 0 : -err;
}

int rfs_rdma_conn_post_write(struct rfs_rdma_conn *conn, uint32_t lkey, uint64_t addr, uint32_t len,
                             uint64_t remote_addr, uint32_t rkey, uint64_t wr_id) {
    struct ibv_sge sge;
    sge.addr = addr;
    sge.length = len;
    sge.lkey = lkey;
    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;
    struct ibv_send_wr *bad = NULL;
    int err = ibv_post_send(conn->id->qp, &wr, &bad);
    return err == 0 ? 0 : -err;
}

int rfs_rdma_conn_peer_addr(const struct rfs_rdma_conn *conn, char *buf, size_t buf_len) {
    struct sockaddr *peer = rdma_get_peer_addr(conn->id);
    if (peer == NULL || buf_len == 0) {
        return -EINVAL;
    }
    char host[INET6_ADDRSTRLEN];
    uint16_t port = 0;
    if (peer->sa_family == AF_INET) {
        const struct sockaddr_in *v4 = (const struct sockaddr_in *)peer;
        inet_ntop(AF_INET, &v4->sin_addr, host, sizeof(host));
        port = ntohs(v4->sin_port);
        snprintf(buf, buf_len, "%s:%u", host, (unsigned)port);
    } else if (peer->sa_family == AF_INET6) {
        const struct sockaddr_in6 *v6 = (const struct sockaddr_in6 *)peer;
        inet_ntop(AF_INET6, &v6->sin6_addr, host, sizeof(host));
        port = ntohs(v6->sin6_port);
        snprintf(buf, buf_len, "[%s]:%u", host, (unsigned)port);
    } else {
        return -EAFNOSUPPORT;
    }
    return 0;
}

void rfs_rdma_conn_abort(struct rfs_rdma_conn *conn) {
    if (conn == NULL || conn->id == NULL || !conn->connected) {
        return;
    }
    // rdma_disconnect moves the queue pair to the error state synchronously
    // before sending the disconnect request, which flushes every posted work
    // request and stops incoming RDMA writes for this connection.
    rdma_disconnect(conn->id);
    conn->connected = 0;
}

void rfs_rdma_conn_close(struct rfs_rdma_conn *conn) {
    if (conn == NULL) {
        return;
    }
    if (conn->id != NULL && conn->connected) {
        rdma_disconnect(conn->id);
        conn->connected = 0;
    }
    // Every event read from the private channel was acknowledged immediately,
    // so rdma_destroy_id cannot wait on an unacknowledged event. Drain what is
    // readable now so the channel is empty when it is destroyed.
    if (conn->channel != NULL) {
        struct rdma_cm_event *event = NULL;
        while (rdma_get_cm_event(conn->channel, &event) == 0) {
            rdma_ack_cm_event(event);
        }
    }
    if (conn->cq != NULL && conn->comp_channel != NULL) {
        unsigned int events = 0;
        for (;;) {
            struct ibv_cq *event_cq = NULL;
            void *event_ctx = NULL;
            if (ibv_get_cq_event(conn->comp_channel, &event_cq, &event_ctx) != 0) {
                break;
            }
            events++;
        }
        if (events != 0) {
            ibv_ack_cq_events(conn->cq, events);
        }
    }
    conn_free(conn);
}

int rfs_rdma_device_reg_mr(struct rfs_rdma_device *device, void *addr, size_t len, struct rfs_rdma_mr **out) {
    struct rfs_rdma_mr *mr = calloc(1, sizeof(*mr));
    if (mr == NULL) {
        return -ENOMEM;
    }
    mr->mr = ibv_reg_mr(device->pd, addr, len, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (mr->mr == NULL) {
        int err = neg_errno();
        free(mr);
        return err;
    }
    *out = mr;
    return 0;
}

uint32_t rfs_rdma_mr_lkey(const struct rfs_rdma_mr *mr) {
    return mr->mr->lkey;
}

uint32_t rfs_rdma_mr_rkey(const struct rfs_rdma_mr *mr) {
    return mr->mr->rkey;
}

void rfs_rdma_mr_dereg(struct rfs_rdma_mr *mr) {
    if (mr == NULL) {
        return;
    }
    if (mr->mr != NULL) {
        ibv_dereg_mr(mr->mr);
    }
    free(mr);
}

const char *rfs_rdma_wc_status_str(uint32_t status) {
    return ibv_wc_status_str((enum ibv_wc_status)status);
}
