#ifndef KUTTIDB_JOB_STATUS_H
#define KUTTIDB_JOB_STATUS_H

/* Typed outcomes shared by the Queue engine, the job modules, the native
 * protocol error byte, the Management API mapping, and every client SDK.
 * The numeric values are part of the wire contract documented in
 * docs/design/PROTOCOL.md; do not reorder. */

#ifdef __cplusplus
extern "C" {
#endif

/* Receipt operation kinds; the kind participates in the canonical semantic
 * identity, so a completion and a state PUT never share one. */
enum {
    JOB_KIND_COMPLETION = 1,
    JOB_KIND_STATE_PUT = 2,
    JOB_KIND_STATE_DELETE = 3
};

typedef enum {
    JOB_OK = 0,
    JOB_UNSUPPORTED_FEATURE = 1,
    JOB_VALIDATION_FAILED = 2,
    JOB_REQUEST_TOO_LARGE = 3,
    JOB_IDEMPOTENCY_CONFLICT = 4,
    JOB_STATE_VERSION_CONFLICT = 5,
    JOB_DELIVERY_EXPIRED = 6,
    JOB_DELIVERY_NOT_OWNED = 7,
    JOB_RESOURCE_EXHAUSTED = 8,
    JOB_OPERATION_IN_PROGRESS = 9,
    JOB_OPERATION_IN_DOUBT = 10,
    JOB_PERSISTENCE_UNAVAILABLE = 11,
    JOB_NOT_FOUND = 12
} JobStatus;

#ifdef __cplusplus
}
#endif

#endif
