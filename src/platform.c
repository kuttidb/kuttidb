#if defined(__linux__)
#define _GNU_SOURCE
#endif

#include "platform.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#if defined(__APPLE__)

#include <sys/event.h>

#define WAKE_IDENT 1u
#define NOTIFY_IDENT 2u

static int change_event(PlatformLoop *loop, uintptr_t ident, int16_t filter,
                        uint16_t flags, uint32_t fflags, intptr_t data,
                        void *userdata) {
    struct kevent change;
    EV_SET(&change, ident, filter, flags, fflags, data, userdata);
    return kevent(loop->fd, &change, 1, NULL, 0, NULL);
}

int platform_loop_init(PlatformLoop *loop) {
    memset(loop, 0, sizeof(*loop));
    loop->aux_fd = -1;
    loop->notify_fd = -1;
    loop->fd = kqueue();
    if (loop->fd < 0) return -1;
    if (change_event(loop, WAKE_IDENT, EVFILT_USER, EV_ADD | EV_ENABLE,
                     0, 0, NULL) < 0 ||
        change_event(loop, NOTIFY_IDENT, EVFILT_USER, EV_ADD | EV_ENABLE,
                     0, 0, NULL) < 0) {
        close(loop->fd);
        loop->fd = -1;
        return -1;
    }
    return 0;
}

void platform_loop_close(PlatformLoop *loop) {
    if (loop->aux_fd >= 0) close(loop->aux_fd);
    if (loop->notify_fd >= 0) close(loop->notify_fd);
    if (loop->fd >= 0) close(loop->fd);
    loop->fd = -1;
    loop->aux_fd = -1;
    loop->notify_fd = -1;
}

int platform_loop_wake(PlatformLoop *loop) {
    return change_event(loop, WAKE_IDENT, EVFILT_USER, EV_ENABLE,
                        NOTE_TRIGGER, 0, NULL);
}

int platform_loop_notify(PlatformLoop *loop) {
    return change_event(loop, NOTIFY_IDENT, EVFILT_USER, EV_ENABLE,
                        NOTE_TRIGGER, 0, NULL);
}

int platform_watch_add(PlatformLoop *loop, int fd, void *userdata,
                       int want_read, int want_write) {
    if (want_read && change_event(loop, (uintptr_t)fd, EVFILT_READ, EV_ADD,
                                  0, 0, userdata) < 0) return -1;
    if (want_write && change_event(loop, (uintptr_t)fd, EVFILT_WRITE, EV_ADD,
                                   0, 0, userdata) < 0) {
        if (want_read) change_event(loop, (uintptr_t)fd, EVFILT_READ,
                                    EV_DELETE, 0, 0, NULL);
        return -1;
    }
    return 0;
}

int platform_watch_update(PlatformLoop *loop, int fd, void *userdata,
                          int want_read, int want_write) {
    int rc = 0;
    if (change_event(loop, (uintptr_t)fd, EVFILT_READ,
                     want_read ? EV_ADD : EV_DELETE, 0, 0,
                     want_read ? userdata : NULL) < 0 && errno != ENOENT)
        rc = -1;
    if (change_event(loop, (uintptr_t)fd, EVFILT_WRITE,
                     want_write ? EV_ADD : EV_DELETE, 0, 0,
                     want_write ? userdata : NULL) < 0 && errno != ENOENT)
        rc = -1;
    return rc;
}

int platform_watch_remove(PlatformLoop *loop, int fd) {
    int rc = 0;
    if (change_event(loop, (uintptr_t)fd, EVFILT_READ, EV_DELETE,
                     0, 0, NULL) < 0 && errno != ENOENT) rc = -1;
    if (change_event(loop, (uintptr_t)fd, EVFILT_WRITE, EV_DELETE,
                     0, 0, NULL) < 0 && errno != ENOENT) rc = -1;
    return rc;
}

int platform_timer_set(PlatformLoop *loop, PlatformTimer *timer, int fd,
                       void *userdata, uint64_t timeout_ms) {
    timer->ident = (uintptr_t)fd;
    timer->active = 0;
    if (timeout_ms == 0) timeout_ms = 1;
    if (timeout_ms > INTPTR_MAX) timeout_ms = INTPTR_MAX;
    /* Darwin's EVFILT_TIMER data is milliseconds when no NOTE_* unit is set. */
    if (change_event(loop, timer->ident, EVFILT_TIMER, EV_ADD | EV_ONESHOT,
                     0, (intptr_t)timeout_ms, userdata) < 0)
        return -1;
    timer->active = 1;
    return 0;
}

int platform_timer_cancel(PlatformLoop *loop, PlatformTimer *timer) {
    if (!timer->active) return 0;
    timer->active = 0;
    if (change_event(loop, timer->ident, EVFILT_TIMER, EV_DELETE,
                     0, 0, NULL) < 0 && errno != ENOENT) return -1;
    return 0;
}

void platform_timer_destroy(PlatformLoop *loop, PlatformTimer *timer) {
    (void)platform_timer_cancel(loop, timer);
    memset(timer, 0, sizeof(*timer));
    timer->fd = -1;
}

int platform_wait(PlatformLoop *loop, PlatformEvent *events, size_t capacity,
                  int timeout_ms) {
    if (capacity == 0 || capacity > INT_MAX) { errno = EINVAL; return -1; }
    struct timespec timeout, *timeout_ptr = NULL;
    if (timeout_ms >= 0) {
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_nsec = (long)(timeout_ms % 1000) * 1000000L;
        timeout_ptr = &timeout;
    }
    struct kevent raw[256];
    if (capacity > sizeof(raw) / sizeof(raw[0])) capacity = sizeof(raw) / sizeof(raw[0]);
    int n = kevent(loop->fd, NULL, 0, raw, (int)capacity, timeout_ptr);
    if (n <= 0) return n;
    for (int i = 0; i < n; i++) {
        events[i].userdata = raw[i].udata;
        events[i].fd = (int)raw[i].ident;
        events[i].flags = raw[i].flags & EV_EOF ? PLATFORM_EVENT_HANGUP : 0;
        if (raw[i].filter == EVFILT_USER)
            events[i].flags |= raw[i].ident == NOTIFY_IDENT
                ? PLATFORM_EVENT_NOTIFY : PLATFORM_EVENT_WAKE;
        else if (raw[i].filter == EVFILT_TIMER) events[i].flags |= PLATFORM_EVENT_TIMER;
        else if (raw[i].filter == EVFILT_WRITE) events[i].flags |= PLATFORM_EVENT_WRITE;
        else if (raw[i].filter == EVFILT_READ) events[i].flags |= PLATFORM_EVENT_READ;
    }
    return n;
}

const char *platform_event_backend(void) { return "kqueue"; }

#elif defined(__linux__)

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>

#define WAKE_TOKEN ((uint64_t)2)
#define NOTIFY_TOKEN ((uint64_t)3)
#define TIMER_TAG  ((uintptr_t)1)

typedef struct LinuxWatch {
    int fd;
    void *userdata;
    struct LinuxWatch *next;
} LinuxWatch;

static uint32_t interest_mask(int want_read, int want_write) {
    uint32_t mask = EPOLLRDHUP;
    if (want_read) mask |= EPOLLIN;
    if (want_write) mask |= EPOLLOUT;
    return mask;
}

static LinuxWatch *find_watch(PlatformLoop *loop, int fd) {
    for (LinuxWatch *watch = loop->watch_list; watch; watch = watch->next)
        if (watch->fd == fd) return watch;
    return NULL;
}

static int ctl_watch(PlatformLoop *loop, int op, LinuxWatch *watch,
                     int want_read, int want_write) {
    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = interest_mask(want_read, want_write);
    event.data.ptr = watch;
    return epoll_ctl(loop->fd, op, watch->fd, &event);
}

int platform_loop_init(PlatformLoop *loop) {
    memset(loop, 0, sizeof(*loop));
    loop->aux_fd = -1;
    loop->notify_fd = -1;
    loop->fd = epoll_create1(EPOLL_CLOEXEC);
    if (loop->fd < 0) return -1;
    int wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd < 0) { close(loop->fd); loop->fd = -1; return -1; }
    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN;
    event.data.u64 = WAKE_TOKEN;
    if (epoll_ctl(loop->fd, EPOLL_CTL_ADD, wake_fd, &event) < 0) {
        close(wake_fd); close(loop->fd); loop->fd = -1; return -1;
    }
    loop->aux_fd = wake_fd;
    int notify_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (notify_fd < 0) {
        close(wake_fd); close(loop->fd);
        loop->fd = -1; loop->aux_fd = -1;
        return -1;
    }
    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN;
    event.data.u64 = NOTIFY_TOKEN;
    if (epoll_ctl(loop->fd, EPOLL_CTL_ADD, notify_fd, &event) < 0) {
        close(notify_fd); close(wake_fd); close(loop->fd);
        loop->fd = -1; loop->aux_fd = -1;
        return -1;
    }
    loop->notify_fd = notify_fd;
    return 0;
}

void platform_loop_close(PlatformLoop *loop) {
    LinuxWatch *watch = loop->watch_list;
    while (watch) {
        LinuxWatch *next = watch->next;
        free(watch);
        watch = next;
    }
    loop->watch_list = NULL;
    if (loop->aux_fd >= 0) close(loop->aux_fd);
    if (loop->notify_fd >= 0) close(loop->notify_fd);
    if (loop->fd >= 0) close(loop->fd);
    loop->fd = -1;
    loop->aux_fd = -1;
    loop->notify_fd = -1;
}

int platform_loop_wake(PlatformLoop *loop) {
    uint64_t one = 1;
    ssize_t rc;
    do rc = write(loop->aux_fd, &one, sizeof(one)); while (rc < 0 && errno == EINTR);
    return rc == (ssize_t)sizeof(one) || (rc < 0 && errno == EAGAIN) ? 0 : -1;
}

int platform_loop_notify(PlatformLoop *loop) {
    uint64_t one = 1;
    ssize_t rc;
    do rc = write(loop->notify_fd, &one, sizeof(one)); while (rc < 0 && errno == EINTR);
    return rc == (ssize_t)sizeof(one) || (rc < 0 && errno == EAGAIN) ? 0 : -1;
}

int platform_watch_add(PlatformLoop *loop, int fd, void *userdata,
                       int want_read, int want_write) {
    if (find_watch(loop, fd)) { errno = EEXIST; return -1; }
    LinuxWatch *watch = calloc(1, sizeof(*watch));
    if (!watch) return -1;
    watch->fd = fd;
    watch->userdata = userdata;
    if (ctl_watch(loop, EPOLL_CTL_ADD, watch, want_read, want_write) < 0) {
        free(watch);
        return -1;
    }
    watch->next = loop->watch_list;
    loop->watch_list = watch;
    return 0;
}

int platform_watch_update(PlatformLoop *loop, int fd, void *userdata,
                          int want_read, int want_write) {
    LinuxWatch *watch = find_watch(loop, fd);
    if (!watch) { errno = ENOENT; return -1; }
    watch->userdata = userdata;
    return ctl_watch(loop, EPOLL_CTL_MOD, watch, want_read, want_write);
}

int platform_watch_remove(PlatformLoop *loop, int fd) {
    LinuxWatch **pp = (LinuxWatch **)&loop->watch_list;
    while (*pp && (*pp)->fd != fd) pp = &(*pp)->next;
    if (!*pp) { errno = ENOENT; return -1; }
    LinuxWatch *watch = *pp;
    int rc = epoll_ctl(loop->fd, EPOLL_CTL_DEL, fd, NULL);
    *pp = watch->next;
    free(watch);
    return rc;
}

int platform_timer_set(PlatformLoop *loop, PlatformTimer *timer, int fd,
                       void *userdata, uint64_t timeout_ms) {
    (void)fd;
    if (timer->fd < 0) {
        timer->fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (timer->fd < 0) return -1;
    }
    struct itimerspec spec;
    memset(&spec, 0, sizeof(spec));
    if (timeout_ms == 0) timeout_ms = 1;
    spec.it_value.tv_sec = (time_t)(timeout_ms / 1000);
    spec.it_value.tv_nsec = (long)(timeout_ms % 1000) * 1000000L;
    if (timerfd_settime(timer->fd, 0, &spec, NULL) < 0) return -1;
    timer->userdata = userdata;
    if (!timer->registered) {
        struct epoll_event event;
        memset(&event, 0, sizeof(event));
        event.events = EPOLLIN;
        event.data.u64 = (uintptr_t)timer | TIMER_TAG;
        if (epoll_ctl(loop->fd, EPOLL_CTL_ADD, timer->fd, &event) < 0) return -1;
        timer->registered = 1;
    }
    timer->active = 1;
    return 0;
}

int platform_timer_cancel(PlatformLoop *loop, PlatformTimer *timer) {
    (void)loop;
    if (timer->fd < 0 || !timer->active) return 0;
    struct itimerspec spec;
    memset(&spec, 0, sizeof(spec));
    timer->active = 0;
    return timerfd_settime(timer->fd, 0, &spec, NULL);
}

void platform_timer_destroy(PlatformLoop *loop, PlatformTimer *timer) {
    (void)platform_timer_cancel(loop, timer);
    if (timer->registered) epoll_ctl(loop->fd, EPOLL_CTL_DEL, timer->fd, NULL);
    if (timer->fd >= 0) close(timer->fd);
    memset(timer, 0, sizeof(*timer));
    timer->fd = -1;
}

int platform_wait(PlatformLoop *loop, PlatformEvent *events, size_t capacity,
                  int timeout_ms) {
    if (capacity == 0 || capacity > INT_MAX) { errno = EINVAL; return -1; }
    struct epoll_event raw[256];
    if (capacity > sizeof(raw) / sizeof(raw[0])) capacity = sizeof(raw) / sizeof(raw[0]);
    int n = epoll_wait(loop->fd, raw, (int)capacity, timeout_ms);
    if (n <= 0) return n;
    for (int i = 0; i < n; i++) {
        uint64_t key = raw[i].data.u64;
        events[i].fd = -1;
        events[i].flags = 0;
        if (key == WAKE_TOKEN) {
            uint64_t discarded;
            (void)read(loop->aux_fd, &discarded, sizeof(discarded));
            events[i].userdata = NULL;
            events[i].flags = PLATFORM_EVENT_WAKE;
            continue;
        }
        if (key == NOTIFY_TOKEN) {
            uint64_t discarded;
            (void)read(loop->notify_fd, &discarded, sizeof(discarded));
            events[i].userdata = NULL;
            events[i].flags = PLATFORM_EVENT_NOTIFY;
            continue;
        }
        if (key & TIMER_TAG) {
            PlatformTimer *timer = (PlatformTimer *)(uintptr_t)(key & ~TIMER_TAG);
            uint64_t ticks;
            events[i].userdata = timer->userdata;
            events[i].flags = PLATFORM_EVENT_TIMER;
            /* timerfd is readable; consuming it avoids a busy loop. */
            (void)read(timer->fd, &ticks, sizeof(ticks));
            continue;
        }
        LinuxWatch *watch = raw[i].data.ptr;
        events[i].fd = watch->fd;
        events[i].userdata = watch->userdata;
        if (raw[i].events & EPOLLIN) events[i].flags |= PLATFORM_EVENT_READ;
        if (raw[i].events & EPOLLOUT) events[i].flags |= PLATFORM_EVENT_WRITE;
        if (raw[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))
            events[i].flags |= PLATFORM_EVENT_HANGUP;
    }
    return n;
}

const char *platform_event_backend(void) { return "epoll"; }

#else
#error "KuttiDB currently provides kqueue and epoll event backends only"
#endif
