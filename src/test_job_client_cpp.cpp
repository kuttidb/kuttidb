// C++ compile/link smoke for the public C companion header: the full job
// completion surface must be usable from C++ with extern "C" linkage.
#include "kuttidb_client.h"

#include <cstdio>

int main() {
    KuttiDBClientOptions opts = {};
    opts.port = 1; // no server: creation and destruction must be safe
    KuttiDBClient *client = kuttidb_client_create(&opts);
    if (!client) return 1;
    int supported = 0;
    // Transport failure is expected here; the point is link + type safety.
    (void)kuttidb_job_check_supported(client, &supported);
    KuttidbJobStatus st = kuttidb_job_check_supported(client, &supported);
    std::printf("status=%s\n", kuttidb_job_status_name(st));
    unsigned char op_id[KUTTIDB_JOB_ID_LEN];
    if (kuttidb_job_new_operation_id(op_id) != 0) {
        kuttidb_client_destroy(client);
        return 1;
    }
    kuttidb_client_destroy(client);
    std::puts("job_client_cpp_test: OK");
    return 0;
}
