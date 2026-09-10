#include "telemetry.h"

#include <assert.h>

int main(void) {
    assert(telemetry_endpoint_valid("https://telemetry.kuttidb.com/v1/report"));
    assert(telemetry_endpoint_valid("https://collector.example:9443/reports/v1"));
    assert(!telemetry_endpoint_valid(NULL));
    assert(!telemetry_endpoint_valid("http://collector.example/v1/report"));
    assert(!telemetry_endpoint_valid("https://collector.example"));
    assert(!telemetry_endpoint_valid("https://token@collector.example/v1/report"));
    assert(!telemetry_endpoint_valid("https://collector.example/v1/report?debug=1"));
    assert(!telemetry_endpoint_valid("https://collector.example/v1/report#anchor"));
    return 0;
}
