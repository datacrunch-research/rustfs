// Fills a GPU (or pinned host) buffer with a deterministic pattern, mints a cuObject PUT
// token for it, prints the token, waits for a line on stdin, then exits. The driver
// compares what the server stored against the same pattern.
#include <cuobjclient.h>
#include <cuda_runtime.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
static ssize_t get_cb(const void*, char*, size_t size, loff_t, const cufileRDMAInfo_t*) { return size; }
static ssize_t put_cb(const void*, const char*, size_t size, loff_t, const cufileRDMAInfo_t*) { return size; }
int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "usage: puthelper <size> <seed> [gpu|host] [dump_file]\n"); return 2; }
    size_t size = strtoull(argv[1], nullptr, 10); unsigned seed = atoi(argv[2]);
    bool gpu = argc < 4 || strcmp(argv[3], "gpu") == 0;
    std::vector<unsigned char> pattern(size);
    unsigned x = seed * 2654435761u + 1;
    for (size_t i = 0; i < size; ++i) { x = x * 1664525u + 1013904223u; pattern[i] = (unsigned char)(x >> 24); }
    if (argc > 4) { FILE* f = fopen(argv[4], "wb"); fwrite(pattern.data(), 1, size, f); fclose(f); }
    CUObjIOOps ops{}; ops.get = get_cb; ops.put = put_cb;
    cuObjClient client(ops, CUOBJ_PROTO_RDMA_DC_V1);
    if (!client.isConnected()) { fprintf(stderr, "client not connected\n"); return 3; }
    void* buf = nullptr;
    if (gpu) { if (cudaMalloc(&buf, size) != cudaSuccess) return 4; cudaMemcpy(buf, pattern.data(), size, cudaMemcpyHostToDevice); }
    else { if (cudaMallocHost(&buf, size) != cudaSuccess) return 4; memcpy(buf, pattern.data(), size); }
    if (client.cuMemObjGetDescriptor(buf, size) != CU_OBJ_SUCCESS) { fprintf(stderr, "descriptor failed\n"); return 5; }
    char* token = nullptr;
    if (client.cuMemObjGetRDMAToken(buf, size, 0, CUOBJ_PUT, &token) != CU_OBJ_SUCCESS) { fprintf(stderr, "token failed\n"); return 6; }
    printf("TOKEN %s\n", token); fflush(stdout);
    char line[64]; if (!fgets(line, sizeof line, stdin)) return 7;
    client.cuMemObjPutRDMAToken(token);
    client.cuMemObjPutDescriptor(buf);
    printf("DONE\n"); fflush(stdout);
    return 0;
}
