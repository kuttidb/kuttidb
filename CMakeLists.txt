cmake_minimum_required(VERSION 3.20)

project(KuttiDB VERSION 0.0.1 LANGUAGES C CXX)

add_compile_definitions(_POSIX_C_SOURCE=200809L _DEFAULT_SOURCE _DARWIN_C_SOURCE)

option(KUTTIDB_TLS "Build TCP TLS support when OpenSSL is available" ON)
option(KUTTIDB_TELEMETRY "Build optional HTTPS telemetry reporter" OFF)
option(KUTTIDB_TELEMETRY_DEFAULT_ON "Default --telemetry to on (official opt-in build; requires KUTTIDB_TELEMETRY=ON)" OFF)

if(NOT APPLE AND NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR
        "This portability foundation currently supports macOS (kqueue) and Linux (epoll). "
        "Windows requires the remaining IOCP, named-pipe, file-locking, and mapping backend.")
endif()

find_package(Threads REQUIRED)

add_library(kuttidb_core STATIC
    src/kuttidb.c
    src/embed.c
    src/embed_kuttidb.c
    src/platform.c
    src/queue.c
    src/job_state.c
    src/job_completion.c
    src/stream.c
)
target_include_directories(kuttidb_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_features(kuttidb_core PUBLIC c_std_11)
target_compile_options(kuttidb_core PRIVATE -Wall -Wextra)
target_link_libraries(kuttidb_core PUBLIC Threads::Threads)

if(KUTTIDB_TLS)
    find_package(OpenSSL QUIET)
endif()

add_executable(kuttidb src/server.c src/admin_http.c src/admin_json.c src/instance_lock.c src/managed_lifecycle.c src/managed_launcher.c src/telemetry.c)
target_compile_features(kuttidb PRIVATE c_std_11)
target_compile_options(kuttidb PRIVATE -Wall -Wextra)
target_link_libraries(kuttidb PRIVATE kuttidb_core)

if(OpenSSL_FOUND)
    target_compile_definitions(kuttidb PRIVATE HAVE_OPENSSL)
    target_link_libraries(kuttidb PRIVATE OpenSSL::SSL OpenSSL::Crypto)
endif()

if(KUTTIDB_TELEMETRY)
    find_package(CURL REQUIRED)
    if(NOT OpenSSL_FOUND)
        message(FATAL_ERROR "KUTTIDB_TELEMETRY requires OpenSSL for installation-id HMAC")
    endif()
    target_compile_definitions(kuttidb PRIVATE HAVE_TELEMETRY)
    target_link_libraries(kuttidb PRIVATE CURL::libcurl)
endif()

if(KUTTIDB_TELEMETRY_DEFAULT_ON)
    if(NOT KUTTIDB_TELEMETRY)
        message(FATAL_ERROR "KUTTIDB_TELEMETRY_DEFAULT_ON requires KUTTIDB_TELEMETRY=ON")
    endif()
    target_compile_definitions(kuttidb PRIVATE KUTTIDB_TELEMETRY_DEFAULT_ON)
endif()

add_library(kuttidb_embed SHARED src/embed.c src/embed_kuttidb.c)
target_compile_features(kuttidb_embed PRIVATE c_std_11)
target_compile_options(kuttidb_embed PRIVATE -Wall -Wextra)
target_link_libraries(kuttidb_embed PRIVATE kuttidb_core)

add_executable(core_test src/test_kuttidb_core.c)
target_compile_features(core_test PRIVATE c_std_11)
target_compile_options(core_test PRIVATE -Wall -Wextra)
target_link_libraries(core_test PRIVATE kuttidb_core)

add_executable(platform_test src/test_platform.c)
target_compile_features(platform_test PRIVATE c_std_11)
target_compile_options(platform_test PRIVATE -Wall -Wextra)
target_link_libraries(platform_test PRIVATE kuttidb_core)

add_executable(managed_lifecycle_test src/test_managed_lifecycle.c src/managed_lifecycle.c)
target_compile_features(managed_lifecycle_test PRIVATE c_std_11)
target_compile_options(managed_lifecycle_test PRIVATE -Wall -Wextra)
target_link_libraries(managed_lifecycle_test PRIVATE Threads::Threads)

add_executable(managed_lock_test src/test_managed_lock.c src/instance_lock.c)
target_compile_features(managed_lock_test PRIVATE c_std_11)
target_compile_options(managed_lock_test PRIVATE -Wall -Wextra)
target_link_libraries(managed_lock_test PRIVATE Threads::Threads)

add_executable(telemetry_config_test src/test_telemetry_config.c src/telemetry.c)
target_compile_features(telemetry_config_test PRIVATE c_std_11)
target_compile_options(telemetry_config_test PRIVATE -Wall -Wextra)
target_link_libraries(telemetry_config_test PRIVATE Threads::Threads)
if(KUTTIDB_TELEMETRY)
    target_compile_definitions(telemetry_config_test PRIVATE HAVE_TELEMETRY)
    target_link_libraries(telemetry_config_test PRIVATE CURL::libcurl OpenSSL::Crypto)
endif()

add_executable(queue_test src/test_queue.c src/queue.c)
target_compile_features(queue_test PRIVATE c_std_11)
target_compile_options(queue_test PRIVATE -Wall -Wextra)
target_link_libraries(queue_test PRIVATE Threads::Threads)

add_executable(queue_failure_test src/test_queue_failures.c src/queue.c)
target_compile_features(queue_failure_test PRIVATE c_std_11)
target_compile_options(queue_failure_test PRIVATE -Wall -Wextra)
target_link_libraries(queue_failure_test PRIVATE Threads::Threads)

add_executable(queue_crash_test src/test_queue_crash.c src/queue.c)
target_compile_features(queue_crash_test PRIVATE c_std_11)
target_compile_options(queue_crash_test PRIVATE -Wall -Wextra)
target_link_libraries(queue_crash_test PRIVATE Threads::Threads)

add_executable(exchange_test src/test_exchange.c src/queue.c)
target_compile_features(exchange_test PRIVATE c_std_11)
target_compile_options(exchange_test PRIVATE -Wall -Wextra)
target_link_libraries(exchange_test PRIVATE Threads::Threads)

add_executable(atomic_test src/test_atomic.c src/queue.c)

add_executable(job_state_test src/test_job_state.c src/job_state.c src/job_completion.c src/queue.c)
target_compile_options(job_state_test PRIVATE -Wall -Wextra)
target_link_libraries(job_state_test PRIVATE Threads::Threads)

add_executable(job_completion_test src/test_job_completion.c src/job_state.c src/job_completion.c src/queue.c)
target_compile_options(job_completion_test PRIVATE -Wall -Wextra)
target_link_libraries(job_completion_test PRIVATE Threads::Threads)

# The crash matrix is only meaningful with the test-only failpoints enabled.
add_executable(job_crash_test src/test_job_crash.c src/job_state.c src/job_completion.c src/queue.c)
target_compile_definitions(job_crash_test PRIVATE KUTTIDB_JOB_FAILPOINTS=1)
target_compile_options(job_crash_test PRIVATE -Wall -Wextra)
target_link_libraries(job_crash_test PRIVATE Threads::Threads)
target_compile_features(atomic_test PRIVATE c_std_11)
target_compile_options(atomic_test PRIVATE -Wall -Wextra)
target_link_libraries(atomic_test PRIVATE Threads::Threads)

add_executable(stream_test src/test_stream.c src/stream.c)
target_compile_features(stream_test PRIVATE c_std_11)
target_compile_options(stream_test PRIVATE -Wall -Wextra)
target_link_libraries(stream_test PRIVATE Threads::Threads)

add_executable(fuzz_test src/test_fuzz.c src/stream.c src/queue.c)
target_compile_features(fuzz_test PRIVATE c_std_11)
target_compile_options(fuzz_test PRIVATE -Wall -Wextra)
target_link_libraries(fuzz_test PRIVATE Threads::Threads)

add_executable(embed_aslr_test src/test_embed_aslr.c)
target_compile_features(embed_aslr_test PRIVATE c_std_11)
target_compile_options(embed_aslr_test PRIVATE -Wall -Wextra)
target_link_libraries(embed_aslr_test PRIVATE kuttidb_core)

add_executable(kuttidb-bench src/kuttidb_bench.c)
target_compile_features(kuttidb-bench PRIVATE c_std_11)
target_compile_options(kuttidb-bench PRIVATE -Wall -Wextra)
target_link_libraries(kuttidb-bench PRIVATE Threads::Threads)

# Public C companion client for atomic job completion (network client; the
# shared-memory embed ABI stays cache-only). TLS support follows the main
# server's OpenSSL discovery.
add_library(kuttidb_client SHARED src/kuttidb_client.c)
target_compile_features(kuttidb_client PUBLIC c_std_11)
target_compile_options(kuttidb_client PRIVATE -Wall -Wextra)
target_link_libraries(kuttidb_client PUBLIC Threads::Threads)
if(OpenSSL_FOUND)
    target_compile_definitions(kuttidb_client PRIVATE HAVE_OPENSSL)
    target_link_libraries(kuttidb_client PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif()

add_executable(job_client_test src/test_job_client_c.c src/kuttidb_client.c)
target_compile_options(job_client_test PRIVATE -Wall -Wextra)
target_link_libraries(job_client_test PRIVATE Threads::Threads)
if(OpenSSL_FOUND)
    target_compile_definitions(job_client_test PRIVATE HAVE_OPENSSL)
    target_link_libraries(job_client_test PRIVATE OpenSSL::SSL OpenSSL::Crypto)
endif()

add_executable(job_client_cpp_test src/test_job_client_cpp.cpp src/kuttidb_client.c)
target_compile_features(job_client_cpp_test PRIVATE cxx_std_17)
target_compile_options(job_client_cpp_test PRIVATE -Wall -Wextra)
target_link_libraries(job_client_cpp_test PRIVATE Threads::Threads)

install(TARGETS kuttidb kuttidb_client
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib)
install(FILES src/kuttidb_client.h DESTINATION include)

# Release builds add -DNDEBUG, which would compile the assert()-based test
# bodies — including setup calls with side effects — out of existence: e.g.
# test_managed_lock.c mints its instance identity inside an assert, and a
# NDEBUG build then feeds uninitialized stack memory to dprintf("%s").
# -UNDEBUG is appended after the per-config flags, so asserts stay live in
# every configuration for every test executable. Production targets are
# unaffected (no assert() in non-test sources).
foreach(kuttidb_test_target
        core_test platform_test managed_lifecycle_test managed_lock_test
        queue_test queue_failure_test queue_crash_test exchange_test
        atomic_test job_state_test job_completion_test job_crash_test
        job_client_test job_client_cpp_test
        stream_test fuzz_test embed_aslr_test)
    target_compile_options(${kuttidb_test_target} PRIVATE -UNDEBUG)
endforeach()

enable_testing()
add_test(NAME core_kuttidb COMMAND core_test)
add_test(NAME platform_events COMMAND platform_test)
add_test(NAME managed_lifecycle COMMAND managed_lifecycle_test)
add_test(NAME managed_lock COMMAND managed_lock_test)
add_test(NAME telemetry_configuration COMMAND telemetry_config_test)
add_test(NAME queue_core COMMAND queue_test)
add_test(NAME queue_failures COMMAND queue_failure_test)
add_test(NAME queue_crash COMMAND queue_crash_test)
add_test(NAME exchange_core COMMAND exchange_test)
add_test(NAME atomic_core COMMAND atomic_test)
add_test(NAME job_state_core COMMAND job_state_test)
add_test(NAME job_completion_core COMMAND job_completion_test)
add_test(NAME job_crash_matrix COMMAND job_crash_test)
add_test(NAME job_client_cpp_link COMMAND job_client_cpp_test)
add_test(NAME stream_core COMMAND stream_test)
add_test(NAME wal_fuzz COMMAND fuzz_test)
add_test(NAME embed_aslr COMMAND embed_aslr_test)
find_package(Python3 COMPONENTS Interpreter QUIET)
if(Python3_Interpreter_FOUND)
    add_test(
        NAME saas_demo
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/examples/saas_demo.py
                --server $<TARGET_FILE:kuttidb>
    )
    add_test(
        NAME management_api
        COMMAND ${CMAKE_COMMAND} -E env "KUTTIDB_SERVER=$<TARGET_FILE:kuttidb>"
                ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/src/test_management_api.py
    )
    add_test(
        NAME managed_server
        COMMAND ${CMAKE_COMMAND} -E env "KUTTIDB_SERVER=$<TARGET_FILE:kuttidb>"
                ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/src/test_managed_server.py
    )
endif()
