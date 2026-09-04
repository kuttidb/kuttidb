#ifndef KUTTIDB_MANAGED_LIFECYCLE_H
#define KUTTIDB_MANAGED_LIFECYCLE_H

#include <stdint.h>

typedef enum { MANAGED_STANDALONE, MANAGED_IDLE } ManagedLifecycleMode;
typedef struct ManagedLifecycle ManagedLifecycle;

void managed_lifecycle_init(ManagedLifecycle *lifecycle, ManagedLifecycleMode mode,
                            uint64_t idle_timeout_ms, uint64_t orphan_timeout_ms);
/* Atomically admits one native lease. Returns 0 once shutdown has begun. */
int managed_lifecycle_connection_open(ManagedLifecycle *lifecycle);
void managed_lifecycle_connection_close(ManagedLifecycle *lifecycle);
/* Returns 1 exactly once when a graceful lifecycle shutdown should begin. */
int managed_lifecycle_should_stop(ManagedLifecycle *lifecycle);
unsigned managed_lifecycle_connections(ManagedLifecycle *lifecycle);
/* Snapshot-only observability helpers. Zero means no idle/orphan deadline is armed. */
uint64_t managed_lifecycle_deadline_remaining_ms(ManagedLifecycle *lifecycle);
int managed_lifecycle_is_stopping(ManagedLifecycle *lifecycle);
const char *managed_lifecycle_name(ManagedLifecycleMode mode);

struct ManagedLifecycle {
    void *opaque[8];
};

#endif
