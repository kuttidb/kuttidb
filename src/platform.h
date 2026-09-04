#ifndef KUTTIDB_PLATFORM_H
#define KUTTIDB_PLATFORM_H

#include <stddef.h>
#include <stdint.h>

/*
 * Small event-polling boundary.  The cache and protocol intentionally do not
 * know which native poller drives their sockets.  Backends are level-triggered
 * so a connection can safely pause reads while its response buffer drains.
 */
enum {
    PLATFORM_EVENT_READ   = 1u << 0,
    PLATFORM_EVENT_WRITE  = 1u << 1,
    PLATFORM_EVENT_HANGUP = 1u << 2,
    PLATFORM_EVENT_TIMER  = 1u << 3,
    PLATFORM_EVENT_WAKE   = 1u << 4,
    /* Non-stopping cross-thread notification (singleflight wakeups). Unlike
     * PLATFORM_EVENT_WAKE it does not ask the loop to shut down. */
    PLATFORM_EVENT_NOTIFY = 1u << 5
};

typedef struct PlatformLoop {
    int fd;
    int aux_fd;      /* backend wake descriptor when one is needed */
    int notify_fd;   /* backend notify descriptor when one is needed */
    void *watch_list; /* backend-private registration list */
} PlatformLoop;

typedef struct PlatformTimer {
    int fd;
    uintptr_t ident;
    int registered;
    int active;
    void *userdata;
} PlatformTimer;

typedef struct PlatformEvent {
    void *userdata;
    int fd;
    unsigned flags;
} PlatformEvent;

int platform_loop_init(PlatformLoop *loop);
void platform_loop_close(PlatformLoop *loop);
int platform_loop_wake(PlatformLoop *loop);
/* Wake a loop without stopping it: the loop receives one PLATFORM_EVENT_NOTIFY. */
int platform_loop_notify(PlatformLoop *loop);

int platform_watch_add(PlatformLoop *loop, int fd, void *userdata,
                       int want_read, int want_write);
int platform_watch_update(PlatformLoop *loop, int fd, void *userdata,
                          int want_read, int want_write);
int platform_watch_remove(PlatformLoop *loop, int fd);

int platform_timer_set(PlatformLoop *loop, PlatformTimer *timer, int fd,
                       void *userdata, uint64_t timeout_ms);
int platform_timer_cancel(PlatformLoop *loop, PlatformTimer *timer);
void platform_timer_destroy(PlatformLoop *loop, PlatformTimer *timer);

/* timeout_ms < 0 waits indefinitely. */
int platform_wait(PlatformLoop *loop, PlatformEvent *events, size_t capacity,
                  int timeout_ms);

const char *platform_event_backend(void);

#endif
