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

#include "cuobj_shim.h"

#include <cuobjserver.h>

#include <cerrno>
#include <cstddef>
#include <exception>
#include <new>
#include <string>

struct rfs_cuobj_server {
    cuObjServer *server;
};

struct rfs_cuobj_buffer {
    struct rdma_buffer *buffer;
};

extern "C" {

int rfs_cuobj_server_create(const char *ip, uint16_t port, int num_dcis, unsigned cq_depth, int max_sge,
                            struct rfs_cuobj_server **out) {
    if (ip == nullptr || out == nullptr) {
        return -EINVAL;
    }
    try {
        cuObjRDMATunable params;
        if (num_dcis > 0) {
            params.setNumDcis(num_dcis);
        }
        if (cq_depth > 0) {
            params.setCqDepth(cq_depth);
        }
        if (max_sge > 0) {
            params.setMaxSge(max_sge);
        }
        auto *wrapper = new (std::nothrow) rfs_cuobj_server{nullptr};
        if (wrapper == nullptr) {
            return -ENOMEM;
        }
        wrapper->server = new cuObjServer(ip, port, CUOBJ_PROTO_RDMA_DC_V1, params);
        *out = wrapper;
        return 0;
    } catch (const std::bad_alloc &) {
        return -ENOMEM;
    } catch (const std::exception &) {
        return -EIO;
    } catch (...) {
        return -EIO;
    }
}

void rfs_cuobj_server_destroy(struct rfs_cuobj_server *server) {
    if (server == nullptr) {
        return;
    }
    try {
        delete server->server;
    } catch (...) {
    }
    delete server;
}

void *rfs_cuobj_alloc_host(struct rfs_cuobj_server *server, size_t size) {
    if (server == nullptr) {
        return nullptr;
    }
    try {
        return server->server->allocHostBuffer(size);
    } catch (...) {
        return nullptr;
    }
}

int rfs_cuobj_register(struct rfs_cuobj_server *server, void *ptr, size_t size, struct rfs_cuobj_buffer **out) {
    if (server == nullptr || ptr == nullptr || out == nullptr) {
        return -EINVAL;
    }
    try {
        struct rdma_buffer *buffer = server->server->registerBuffer(ptr, size);
        if (buffer == nullptr) {
            return -ENOMEM;
        }
        auto *wrapper = new (std::nothrow) rfs_cuobj_buffer{buffer};
        if (wrapper == nullptr) {
            server->server->deRegisterBuffer(buffer);
            return -ENOMEM;
        }
        *out = wrapper;
        return 0;
    } catch (...) {
        return -EIO;
    }
}

void rfs_cuobj_deregister(struct rfs_cuobj_server *server, struct rfs_cuobj_buffer *buffer) {
    if (server == nullptr || buffer == nullptr) {
        return;
    }
    try {
        server->server->deRegisterBuffer(buffer->buffer);
    } catch (...) {
    }
    delete buffer;
}

int rfs_cuobj_sg_buffer(struct rfs_cuobj_server *server, struct rfs_cuobj_buffer *base,
                        const struct rfs_cuobj_sg_entry *entries, size_t count, struct rfs_cuobj_buffer **out) {
    if (server == nullptr || base == nullptr || entries == nullptr || count == 0 || out == nullptr) {
        return -EINVAL;
    }
    try {
        std::vector<cuObjScatterGatherEntryWithOffset_t> sglist;
        sglist.reserve(count);
        for (size_t i = 0; i < count; i++) {
            cuObjScatterGatherEntryWithOffset_t entry;
            entry.offset = static_cast<loff_t>(entries[i].offset);
            entry.size = entries[i].size;
            sglist.push_back(entry);
        }
        struct rdma_buffer *buffer = server->server->getRDMABufferFromSgList(base->buffer, sglist);
        if (buffer == nullptr) {
            return -ENOMEM;
        }
        auto *wrapper = new (std::nothrow) rfs_cuobj_buffer{buffer};
        if (wrapper == nullptr) {
            server->server->putRDMABufferFromSgList(buffer);
            return -ENOMEM;
        }
        *out = wrapper;
        return 0;
    } catch (...) {
        return -EIO;
    }
}

void rfs_cuobj_sg_buffer_put(struct rfs_cuobj_server *server, struct rfs_cuobj_buffer *sg) {
    if (server == nullptr || sg == nullptr) {
        return;
    }
    try {
        server->server->putRDMABufferFromSgList(sg->buffer);
    } catch (...) {
    }
    delete sg;
}

int rfs_cuobj_channel_alloc(struct rfs_cuobj_server *server, uint16_t *out) {
    if (server == nullptr || out == nullptr) {
        return -EINVAL;
    }
    try {
        uint16_t channel = server->server->allocateChannelId();
        if (channel == INVALID_CHANNEL_ID) {
            return -EBUSY;
        }
        *out = channel;
        return 0;
    } catch (...) {
        return -EIO;
    }
}

void rfs_cuobj_channel_free(struct rfs_cuobj_server *server, uint16_t channel) {
    if (server == nullptr) {
        return;
    }
    try {
        server->server->freeChannelId(channel);
    } catch (...) {
    }
}

ssize_t rfs_cuobj_get(struct rfs_cuobj_server *server, const char *key, struct rfs_cuobj_buffer *buffer,
                      uint64_t local_offset, uint64_t remote_addr, size_t size, const char *descriptor,
                      uint16_t channel, int *verbs_status) {
    if (server == nullptr || buffer == nullptr || descriptor == nullptr) {
        return -EINVAL;
    }
    try {
        ibv_wc_status status = IBV_WC_SUCCESS;
        ssize_t written = server->server->handleGetObject(std::string(key ? key : ""), buffer->buffer, remote_addr,
                                                          size, std::string(descriptor), channel, local_offset,
                                                          &status, nullptr);
        if (verbs_status != nullptr) {
            *verbs_status = static_cast<int>(status);
        }
        return written;
    } catch (...) {
        return -EIO;
    }
}

ssize_t rfs_cuobj_put(struct rfs_cuobj_server *server, const char *key, struct rfs_cuobj_buffer *buffer,
                      uint64_t local_offset, uint64_t remote_addr, size_t size, const char *descriptor,
                      uint16_t channel, int *verbs_status) {
    if (server == nullptr || buffer == nullptr || descriptor == nullptr) {
        return -EINVAL;
    }
    try {
        ibv_wc_status status = IBV_WC_SUCCESS;
        ssize_t read = server->server->handlePutObject(std::string(key ? key : ""), buffer->buffer, remote_addr, size,
                                                       std::string(descriptor), channel, local_offset, &status,
                                                       nullptr);
        if (verbs_status != nullptr) {
            *verbs_status = static_cast<int>(status);
        }
        return read;
    } catch (...) {
        return -EIO;
    }
}

ssize_t rfs_cuobj_get_async(struct rfs_cuobj_server *server, const char *key, struct rfs_cuobj_buffer *buffer,
                            uint64_t local_offset, uint64_t remote_addr, size_t size, const char *descriptor,
                            uint16_t channel, void *handle) {
    if (server == nullptr || buffer == nullptr || descriptor == nullptr || handle == nullptr) {
        return -EINVAL;
    }
    try {
        ibv_wc_status status = IBV_WC_SUCCESS;
        return server->server->handleGetObject(std::string(key ? key : ""), buffer->buffer, remote_addr, size,
                                               std::string(descriptor), channel, local_offset, &status, handle);
    } catch (...) {
        return -EIO;
    }
}

int rfs_cuobj_channel_completion_fd(struct rfs_cuobj_server *server, uint16_t channel) {
    if (server == nullptr) {
        return -1;
    }
    try {
        return server->server->getCompletionFd(channel);
    } catch (...) {
        return -1;
    }
}

int rfs_cuobj_process_completions(struct rfs_cuobj_server *server, int fd, struct rfs_cuobj_completion *out,
                                  int max) {
    if (server == nullptr || out == nullptr || max <= 0) {
        return -EINVAL;
    }
    static_assert(sizeof(cuObjAsyncEvent_t) == sizeof(struct rfs_cuobj_completion),
                  "rfs_cuobj_completion mirrors cuObjAsyncEvent_t");
    static_assert(offsetof(cuObjAsyncEvent_t, async_handle) == offsetof(struct rfs_cuobj_completion, handle) &&
                      offsetof(cuObjAsyncEvent_t, status) == offsetof(struct rfs_cuobj_completion, status),
                  "rfs_cuobj_completion mirrors cuObjAsyncEvent_t");
    try {
        return server->server->processCompletions(reinterpret_cast<cuObjAsyncEvent_t *>(out), max, fd);
    } catch (...) {
        return -EIO;
    }
}

int rfs_cuobj_version(void) {
    return CUOBJ_SERVER_MAJOR_VERSION * 1000 + CUOBJ_SERVER_MINOR_VERSION;
}

} // extern "C"
