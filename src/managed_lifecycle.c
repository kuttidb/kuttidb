#include "managed_lifecycle.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    pthread_mutex_t mutex;
    ManagedLifecycleMode mode;
    uint64_t idle_ms, orphan_ms, started_ms, deadline_ms;
    unsigned connections;
    int seen_connection, stopping;
} State;

static uint64_t now_ms(void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000; }
static State *state(ManagedLifecycle *l) { return *(State **)&l->opaque[0]; }

void managed_lifecycle_init(ManagedLifecycle *l, ManagedLifecycleMode mode, uint64_t idle, uint64_t orphan) {
    memset(l, 0, sizeof *l);
    State *s = calloc(1, sizeof *s); if (!s) return;
    *(State **)&l->opaque[0] = s; pthread_mutex_init(&s->mutex, NULL);
    s->mode = mode; s->idle_ms = idle; s->orphan_ms = orphan; s->started_ms = now_ms();
    if (mode == MANAGED_IDLE) s->deadline_ms = s->started_ms + orphan;
}
int managed_lifecycle_connection_open(ManagedLifecycle *l) { State *s = state(l); int admitted = 0; pthread_mutex_lock(&s->mutex); if (!s->stopping) { s->connections++; s->seen_connection = 1; s->deadline_ms = 0; admitted = 1; } pthread_mutex_unlock(&s->mutex); return admitted; }
void managed_lifecycle_connection_close(ManagedLifecycle *l) { State *s = state(l); pthread_mutex_lock(&s->mutex); if (s->connections) s->connections--; if (s->mode == MANAGED_IDLE && !s->connections && !s->stopping) s->deadline_ms = now_ms() + s->idle_ms; pthread_mutex_unlock(&s->mutex); }
int managed_lifecycle_should_stop(ManagedLifecycle *l) { State *s = state(l); int stop = 0; pthread_mutex_lock(&s->mutex); if (s->mode == MANAGED_IDLE && !s->stopping && !s->connections && s->deadline_ms && now_ms() >= s->deadline_ms) { s->stopping = 1; stop = 1; } pthread_mutex_unlock(&s->mutex); return stop; }
unsigned managed_lifecycle_connections(ManagedLifecycle *l) { State *s = state(l); pthread_mutex_lock(&s->mutex); unsigned n=s->connections; pthread_mutex_unlock(&s->mutex); return n; }
uint64_t managed_lifecycle_deadline_remaining_ms(ManagedLifecycle *l) { State *s = state(l); pthread_mutex_lock(&s->mutex); uint64_t now=now_ms(), remaining=s->deadline_ms>now?s->deadline_ms-now:0; pthread_mutex_unlock(&s->mutex); return remaining; }
int managed_lifecycle_is_stopping(ManagedLifecycle *l) { State *s = state(l); pthread_mutex_lock(&s->mutex); int stopping=s->stopping; pthread_mutex_unlock(&s->mutex); return stopping; }
const char *managed_lifecycle_name(ManagedLifecycleMode mode) { return mode == MANAGED_IDLE ? "managed-idle" : "standalone"; }
