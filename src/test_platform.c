#include "platform.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

static int find_event(const PlatformEvent *events, int count, unsigned flag,
                      void *userdata, int fd) {
    for (int i = 0; i < count; i++) {
        if ((events[i].flags & flag) && events[i].userdata == userdata &&
            (fd < 0 || events[i].fd == fd))
            return 1;
    }
    return 0;
}

int main(void) {
    PlatformLoop loop;
    PlatformTimer timer = {.fd = -1};
    PlatformEvent events[8];
    int pipefd[2];
    int watch_token = 1;
    int timer_token = 2;

    if (platform_loop_init(&loop) < 0 || pipe(pipefd) < 0) {
        perror("platform init");
        return 1;
    }
    if (platform_watch_add(&loop, pipefd[0], &watch_token, 1, 0) < 0 ||
        write(pipefd[1], "x", 1) != 1) {
        perror("platform watch");
        return 1;
    }
    int count = platform_wait(&loop, events, 8, 1000);
    if (count < 0 || !find_event(events, count, PLATFORM_EVENT_READ,
                                 &watch_token, pipefd[0])) {
        fprintf(stderr, "read watch event missing\n");
        return 1;
    }
    char byte;
    if (read(pipefd[0], &byte, 1) != 1 ||
        platform_watch_remove(&loop, pipefd[0]) < 0) {
        perror("platform watch cleanup");
        return 1;
    }

    if (platform_timer_set(&loop, &timer, pipefd[0], &timer_token, 10) < 0) {
        perror("platform timer");
        return 1;
    }
    count = platform_wait(&loop, events, 8, 1000);
    if (count < 0 || !find_event(events, count, PLATFORM_EVENT_TIMER,
                                 &timer_token, -1)) {
        fprintf(stderr, "timer event missing\n");
        return 1;
    }
    platform_timer_destroy(&loop, &timer);

    if (platform_loop_wake(&loop) < 0) {
        perror("platform wake");
        return 1;
    }
    count = platform_wait(&loop, events, 8, 1000);
    if (count < 0 || !find_event(events, count, PLATFORM_EVENT_WAKE, NULL, -1)) {
        fprintf(stderr, "wake event missing\n");
        return 1;
    }

    close(pipefd[0]);
    close(pipefd[1]);
    platform_loop_close(&loop);
    puts("PLATFORM EVENT TESTS PASSED");
    return 0;
}
