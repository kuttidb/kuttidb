cmake_minimum_required(VERSION 3.20)

project(KuttiDB VERSION 0.0.1 LANGUAGES C)

add_compile_definitions(_POSIX_C_SOURCE=200809L _DEFAULT_SOURCE _DARWIN_C_SOURCE)

option(KUTTIDB_TLS "Build TCP TLS support when OpenSSL is available" ON)

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
    src/stream.c
)
target_include_directories(kuttidb_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_features(kuttidb_core PUBLIC c_std_11)
target_compile_options(kuttidb_core PRIVATE -Wall -Wextra)
target_link_libraries(kuttidb_core PUBLIC Threads::Threads)

if(KUTTIDB_TLS)
    find_package(OpenSSL QUIET)
endif()

add_executable(kuttidb src/server.c src/admin_http.c src/admin_json.c src/instance_lock.c src/managed_lifecycle.c src/managed_launcher.c)
target_compile_features(kuttidb PRIVATE c_std_11)
target_compile_options(kuttidb PRIVATE -Wall -Wextra)
target_link_libraries(kuttidb PRIVATE kuttidb_core)

if(OpenSSL_FOUND)
    target_compile_definitions(kuttidb PRIVATE HAVE_OPENSSL)
    target_link_libraries(kuttidb PRIVATE OpenSSL::SSL OpenSSL::Crypto)
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

enable_testing()
add_test(NAME core_kuttidb COMMAND core_test)
add_test(NAME platform_events COMMAND platform_test)
add_test(NAME managed_lifecycle COMMAND managed_lifecycle_test)
add_test(NAME managed_lock COMMAND managed_lock_test)
add_test(NAME queue_core COMMAND queue_test)
add_test(NAME queue_failures COMMAND queue_failure_test)
add_test(NAME queue_crash COMMAND queue_crash_test)
add_test(NAME exchange_core COMMAND exchange_test)
add_test(NAME atomic_core COMMAND atomic_test)
add_test(NAME stream_core COMMAND stream_test)
add_test(NAME wal_fuzz COMMAND fuzz_test)
add_test(NAME embed_aslr COMMAND embed_aslr_test)
find_package(Python3 COMPONENTS Interpreter QUIET)
if(Python3_Interpreter_FOUND)
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
