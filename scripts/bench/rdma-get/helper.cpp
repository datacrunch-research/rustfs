// Mints a cuObject GET token for a GPU (or pinned host) buffer, prints it, waits for
// a line on stdin, then dumps the buffer to a file so the driver can verify it.
#include <cuobjclient.h>
#include <cuda_runtime.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
static ssize_t get_cb(const void*, char*, size_t size, loff_t, const cufileRDMAInfo_t*) { return size; }
static ssize_t put_cb(const void*, const char*, size_t size, loff_t, const cufileRDMAInfo_t*) { return size; }
int main(int argc, char** argv) {
    if (argc < 4) { fprintf(stderr, "usage: helper <size> <out_file> <gpu|host>\n"); return 2; }
    size_t size = strtoull(argv[1], nullptr, 10);
    bool gpu = strcmp(argv[3], "gpu") == 0;
    CUObjIOOps ops{}; ops.get = get_cb; ops.put = put_cb;
    cuObjClient client(ops, CUOBJ_PROTO_RDMA_DC_V1);
    if (!client.isConnected()) { fprintf(stderr, "client not connected\n"); return 3; }
    void* buf = nullptr;
    if (gpu) { if (cudaMalloc(&buf, size) != cudaSuccess) return 4; cudaMemset(buf, 0, size); }
    else { if (cudaMallocHost(&buf, size) != cudaSuccess) return 4; memset(buf, 0, size); }
    if (client.cuMemObjGetDescriptor(buf, size) != CU_OBJ_SUCCESS) { fprintf(stderr, "descriptor failed\n"); return 5; }
    char* token = nullptr;
    if (client.cuMemObjGetRDMAToken(buf, size, 0, CUOBJ_GET, &token) != CU_OBJ_SUCCESS) { fprintf(stderr, "token failed\n"); return 6; }
    printf("TOKEN %s\n", token); fflush(stdout);
    char line[64]; if (!fgets(line, sizeof line, stdin)) return 7;
    std::string out(size, '\0');
    if (gpu) cudaMemcpy(out.data(), buf, size, cudaMemcpyDeviceToHost); else memcpy(out.data(), buf, size);
    FILE* f = fopen(argv[2], "wb"); fwrite(out.data(), 1, size, f); fclose(f);
    client.cuMemObjPutRDMAToken(token);
    client.cuMemObjPutDescriptor(buf);
    printf("DUMPED\n"); fflush(stdout);
    return 0;
}
