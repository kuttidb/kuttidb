#include "managed_lifecycle.h"
#include <assert.h>
#include <stdio.h>
#include <unistd.h>

int main(void) {
    ManagedLifecycle standalone, managed;
    managed_lifecycle_init(&standalone, MANAGED_STANDALONE, 1, 1);
    usleep(3000);
    assert(!managed_lifecycle_should_stop(&standalone));

    managed_lifecycle_init(&managed, MANAGED_IDLE, 2, 2);
    assert(managed_lifecycle_connection_open(&managed));
    assert(managed_lifecycle_connections(&managed) == 1);
    usleep(3000);
    assert(!managed_lifecycle_should_stop(&managed));
    managed_lifecycle_connection_close(&managed);
    assert(managed_lifecycle_connections(&managed) == 0);
    usleep(3000);
    assert(managed_lifecycle_should_stop(&managed));
    assert(!managed_lifecycle_should_stop(&managed));
    assert(!managed_lifecycle_connection_open(&managed));
    assert(managed_lifecycle_connections(&managed) == 0);
    puts("managed lifecycle tests passed");
    return 0;
}
