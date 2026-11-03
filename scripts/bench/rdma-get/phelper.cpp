// Mints N cuObject GET tokens for N equal slices of one GPU buffer, prints them,
// waits for a line on stdin, then dumps the buffer.
#include <cuobjclient.h>
#include <cuda_runtime.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
static ssize_t get_cb(const void*, char*, size_t size, loff_t, const cufileRDMAInfo_t*) { return size; }
static ssize_t put_cb(const void*, const char*, size_t size, loff_t, const cufileRDMAInfo_t*) { return size; }
int main(int argc, char** argv) {
    if (argc < 4) { fprintf(stderr, "usage: phelper <size> <out_file> <nslices> [gpu|host]\n"); return 2; }
    size_t size = strtoull(argv[1], nullptr, 10);
    int n = atoi(argv[3]);
    bool gpu = argc < 5 || strcmp(argv[4], "gpu") == 0;
    CUObjIOOps ops{}; ops.get = get_cb; ops.put = put_cb;
    cuObjClient client(ops, CUOBJ_PROTO_RDMA_DC_V1);
    if (!client.isConnected()) { fprintf(stderr, "client not connected\n"); return 3; }
    void* buf = nullptr;
    if (gpu) { if (cudaMalloc(&buf, size) != cudaSuccess) return 4; cudaMemset(buf, 0, size); }
    else { if (cudaMallocHost(&buf, size) != cudaSuccess) return 4; memset(buf, 0, size); }
    // Register each slice on its own: a single registration is limited to < 4 GiB.
    size_t slice = (size + n - 1) / n;
    std::vector<char*> tokens; std::vector<void*> regs;
    for (int i = 0; i < n; ++i) {
        size_t off = (size_t)i * slice; size_t len = off + slice > size ? size - off : slice;
        void* base = (char*)buf + off;
        if (client.cuMemObjGetDescriptor(base, len) != CU_OBJ_SUCCESS) { fprintf(stderr, "descriptor %d failed\n", i); return 5; }
        regs.push_back(base);
        char* token = nullptr;
        if (client.cuMemObjGetRDMAToken(base, len, 0, CUOBJ_GET, &token) != CU_OBJ_SUCCESS) { fprintf(stderr, "token %d failed\n", i); return 6; }
        printf("TOKEN %zu %zu %s\n", off, len, token); tokens.push_back(token);
    }
    printf("READY\n"); fflush(stdout);
    char line[64]; if (!fgets(line, sizeof line, stdin)) return 7;
    std::string out(size, '\0');
    if (gpu) cudaMemcpy(out.data(), buf, size, cudaMemcpyDeviceToHost); else memcpy(out.data(), buf, size);
    FILE* f = fopen(argv[2], "wb"); fwrite(out.data(), 1, size, f); fclose(f);
    for (char* t : tokens) client.cuMemObjPutRDMAToken(t);
    for (void* r : regs) client.cuMemObjPutDescriptor(r);
    printf("DUMPED\n"); fflush(stdout);
    return 0;
}
