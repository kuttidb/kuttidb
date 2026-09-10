#ifndef KUTTIDB_TELEMETRY_H
#define KUTTIDB_TELEMETRY_H

#include <stdatomic.h>
#include <signal.h>

typedef struct {
    int enabled;
    const char *endpoint;
    const char *state_dir;
} TelemetryConfig;

/* Validate the narrow v1 collector URL form without initializing telemetry. */
int telemetry_endpoint_valid(const char *endpoint);

/* All functions are no-ops in non-telemetry builds. */
int telemetry_start(const TelemetryConfig *config, const _Atomic unsigned int *connections,
                    const volatile sig_atomic_t *stop_flag);
void telemetry_stop(void);

#endif
