CC = cc
CFLAGS = -O2 -Wall -Wextra -std=c11 -pthread -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE -D_DARWIN_C_SOURCE
LDFLAGS = -pthread
TLS ?= 1

# Go caches must live under a sandbox-writable root (this workspace or the
# platform temp dir); the home-directory defaults are not writable when the
# session runs under the workspace-write file policy. ?= lets the
# environment override.
GO_TMP_ROOT = $(or $(TMPDIR),/tmp)
export GOCACHE ?= $(GO_TMP_ROOT)/kuttidb-go-build
export GOMODCACHE ?= $(GO_TMP_ROOT)/kuttidb-go-mod

ifeq ($(TLS),1)
OPENSSL_PREFIX ?= $(shell pkg-config --variable=prefix openssl 2>/dev/null || \
	(test -d /opt/homebrew/opt/openssl@3 && echo /opt/homebrew/opt/openssl@3) || \
	(test -d /usr/local/opt/openssl@3 && echo /usr/local/opt/openssl@3))
ifneq ($(strip $(OPENSSL_PREFIX)),)
TLS_CFLAGS = -DHAVE_OPENSSL -I$(OPENSSL_PREFIX)/include
TLS_LIBS = -L$(OPENSSL_PREFIX)/lib -lssl -lcrypto
CFLAGS += $(TLS_CFLAGS)
endif
endif

# The embedded shared library is platform-shaped: a dylib on macOS, an so on
# Linux/Alpine. CMake produces the same artifacts natively.
ifeq ($(shell uname -s),Darwin)
EMBED_LIB = libkuttidb_embed.dylib
else
EMBED_LIB = libkuttidb_embed.so
endif

all: kuttidb kuttidb-bench $(EMBED_LIB)

# The embedded shared library is platform-shaped: a dylib on macOS, an so on
# Linux/Alpine. CMake produces the same artifacts natively.
ifeq ($(shell uname -s),Darwin)
EMBED_LIB = libkuttidb_embed.dylib
else
EMBED_LIB = libkuttidb_embed.so
endif

core_test: src/test_kuttidb_core.c src/kuttidb.c src/embed.c src/embed_kuttidb.c src/kuttidb.h src/kuttidb_int.h src/embed_int.h
	$(CC) $(CFLAGS) -Isrc -o $@ src/test_kuttidb_core.c src/kuttidb.c src/embed.c src/embed_kuttidb.c $(LDFLAGS)

platform_test: src/test_platform.c src/platform.c src/platform.h
	$(CC) $(CFLAGS) -Isrc -o $@ src/test_platform.c src/platform.c $(LDFLAGS)

managed_lifecycle_test: src/test_managed_lifecycle.c src/managed_lifecycle.c src/managed_lifecycle.h
	$(CC) $(CFLAGS) -Isrc -o $@ src/test_managed_lifecycle.c src/managed_lifecycle.c $(LDFLAGS)

managed_lock_test: src/test_managed_lock.c src/instance_lock.c src/instance_lock.h
	$(CC) $(CFLAGS) -Isrc -o $@ src/test_managed_lock.c src/instance_lock.c $(LDFLAGS)

queue_test: src/test_queue.c src/queue.c src/queue.h
	$(CC) $(CFLAGS) -Isrc -o $@ src/test_queue.c src/queue.c $(LDFLAGS)

queue_failure_test: src/test_queue_failures.c src/queue.c src/queue.h
	$(CC) $(CFLAGS) -Isrc -o $@ src/test_queue_failures.c src/queue.c $(LDFLAGS)

queue_crash_test: src/test_queue_crash.c src/queue.c src/queue.h
	$(CC) $(CFLAGS) -Isrc -o $@ src/test_queue_crash.c src/queue.c $(LDFLAGS)

queue_concurrency_test: src/test_queue_concurrency.c src/queue.c src/queue.h
	$(CC) $(CFLAGS) -Isrc -o $@ src/test_queue_concurrency.c src/queue.c $(LDFLAGS)

exchange_test: src/test_exchange.c src/queue.c src/queue.h
	$(CC) $(CFLAGS) -Isrc -o $@ src/test_exchange.c src/queue.c $(LDFLAGS)

atomic_test: src/test_atomic.c src/queue.c src/queue.h
	$(CC) $(CFLAGS) -Isrc -o $@ src/test_atomic.c src/queue.c $(LDFLAGS)

stream_test: src/test_stream.c src/stream.c src/stream.h
	$(CC) $(CFLAGS) -Isrc -o $@ src/test_stream.c src/stream.c $(LDFLAGS)

fuzz_test: src/test_fuzz.c src/stream.c src/queue.c src/stream.h src/queue.h
	$(CC) $(CFLAGS) -Isrc -o $@ src/test_fuzz.c src/stream.c src/queue.c $(LDFLAGS)

embed_aslr_test: src/test_embed_aslr.c src/kuttidb.c src/embed.c src/embed_kuttidb.c src/embed.h src/embed_int.h
	$(CC) $(CFLAGS) -Isrc -o $@ src/test_embed_aslr.c src/kuttidb.c src/embed.c src/embed_kuttidb.c $(LDFLAGS)

kuttidb: src/kuttidb.o src/server.o src/admin_http.o src/admin_json.o src/embed.o src/embed_kuttidb.o src/platform.o src/queue.o src/stream.o src/instance_lock.o src/managed_lifecycle.o src/managed_launcher.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(TLS_LIBS)

libkuttidb_embed.dylib: src/kuttidb.o src/embed.o src/embed_kuttidb.o
	$(CC) $(CFLAGS) -dynamiclib -Wl,-install_name,@rpath/libkuttidb_embed.dylib -o $@ $^ $(LDFLAGS)

libkuttidb_embed.so: src/kuttidb.c src/embed.c src/embed_kuttidb.c src/kuttidb.h src/kuttidb_int.h src/embed_int.h
	$(CC) $(CFLAGS) -fPIC -shared -o $@ src/kuttidb.c src/embed.c src/embed_kuttidb.c $(LDFLAGS)

kuttidb-bench: src/kuttidb_bench.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

src/%.o: src/%.c src/kuttidb.h src/kuttidb_int.h
	$(CC) $(CFLAGS) -c -o $@ $<

src/server.o: src/server.c src/kuttidb.h src/kuttidb_int.h src/embed.h src/platform.h src/queue.h src/stream.h src/admin_http.h
src/admin_http.o: src/admin_http.c src/admin_http.h src/admin_json.h src/kuttidb.h src/queue.h src/stream.h
src/admin_json.o: src/admin_json.c src/admin_json.h
src/platform.o: src/platform.c src/platform.h
src/instance_lock.o: src/instance_lock.c src/instance_lock.h
src/managed_lifecycle.o: src/managed_lifecycle.c src/managed_lifecycle.h
src/managed_launcher.o: src/managed_launcher.c src/managed_launcher.h src/instance_lock.h

clean:
	rm -f kuttidb kuttidb_sanitize kuttidb-bench core_test core_test_sanitize platform_test queue_test queue_failure_test queue_crash_test queue_concurrency_test exchange_test atomic_test stream_test stream_test_sanitize fuzz_test fuzz_test_sanitize embed_aslr_test \
		libkuttidb_embed.dylib libkuttidb_embed.so managed_lifecycle_test managed_lock_test src/*.o clients/java/*.class

install: all
	install -m 0755 kuttidb /usr/local/bin/kuttidb
	install -m 0755 kuttidb-cli /usr/local/bin/kuttidb-cli

test: all core_test platform_test managed_lifecycle_test managed_lock_test queue_test queue_failure_test queue_crash_test queue_concurrency_test exchange_test atomic_test stream_test fuzz_test embed_aslr_test
	@./core_test
	@./platform_test
	@./managed_lifecycle_test
	@./managed_lock_test
	@./queue_test
	@./queue_failure_test
	@./queue_crash_test
	@./queue_concurrency_test
	@./exchange_test
	@./atomic_test
	@./stream_test
	@./fuzz_test
	@python3 src/test_stream_protocol.py
	@./embed_aslr_test
	@python3 src/test_client.py 7391
	@python3 src/test_local_transport.py
	@python3 src/test_queue_protocol.py
	@python3 src/test_exchange_protocol.py
	@python3 src/test_atomic_protocol.py
	@python3 src/test_stampede_protocol.py
	@python3 src/test_persistence.py
	@python3 src/test_ttl.py
	@python3 src/test_embed.py
	@python3 src/test_security.py
	@python3 src/test_management_api.py
	@python3 src/test_managed_server.py
	@python3 src/test_protocol_fuzz.py
	@python3 src/test_reliability.py
	@set -e; tmp=$$(mktemp -d); ./kuttidb 7394 $$tmp/kuttidb.wal 100 \
		--queue-wal $$tmp/queue.wal 2>/dev/null & server_pid=$$!; \
	trap 'kill $$server_pid 2>/dev/null || true; rm -rf $$tmp' EXIT; sleep 0.7; \
	cd clients/go && go vet ./... && go run ./cmd/smoketest; \
	cd ../java && javac -encoding UTF-8 KuttiDBClient.java Smoke.java && java -cp . Smoke; \
	cd ../rust && cargo build --quiet && ./target/debug/smoketest; \
	if command -v node >/dev/null 2>&1; then cd ../nodejs && node smoke.js 7394; else echo "node not installed: skipping Node.js client smoke"; fi; \
	kill $$server_pid; wait $$server_pid || true

# The default C/Python gate deliberately has no SDK toolchain requirement.
# Run this target in release CI images to exercise the opt-in lifecycle through
# every supported SDK against the same freshly-built local executable.
managed-sdk-test: kuttidb $(EMBED_LIB)
	@set -e; tmp=$$(mktemp -d); trap 'rm -rf $$tmp' EXIT; \
	KUTTIDB_SERVER="$$PWD/kuttidb" python3 src/test_managed_server.py; \
	KUTTIDB_SERVER="$$PWD/kuttidb" node clients/nodejs/managed_smoke.js "$$tmp/node"; \
	KUTTIDB_SERVER="$$PWD/kuttidb" node clients/nodejs/managed_smoke.js "$$tmp/node-tcp" tcp; \
	cd clients/go && KUTTIDB_MANAGED_INTEGRATION=1 KUTTIDB_SERVER="$$PWD/../../kuttidb" go test -run TestManagedLifecycleIntegration -count=1; \
	cd ../java && javac -encoding UTF-8 KuttiDBClient.java ManagedSmoke.java && java -cp . ManagedSmoke "$$tmp/java" "$$PWD/../../kuttidb"; \
	java -cp . ManagedSmoke "$$tmp/java-tcp" "$$PWD/../../kuttidb" tcp; \
	cd ../rust && KUTTIDB_MANAGED_INTEGRATION=1 KUTTIDB_SERVER="$$PWD/../../kuttidb" cargo test managed_lifecycle_integration -- --nocapture

bench: kuttidb kuttidb-bench
	@set -e; ./kuttidb 7392 - 100 2>/dev/null & server_pid=$$!; \
	trap 'kill $$server_pid 2>/dev/null || true' EXIT; sleep 0.7; ./kuttidb-bench 7392 8 100000; \
	python3 src/bench_multi.py 7392 16 25000; \
	cd clients/go && go run ./cmd/bench 7392 8 50000; \
	kill $$server_pid; wait $$server_pid || true

bench-stream: src/bench_stream.c src/stream.c src/stream.h
	$(CC) $(CFLAGS) -Isrc -o bench_stream src/bench_stream.c src/stream.c $(LDFLAGS)
	@tmp=$$(mktemp -d); ./bench_stream $$tmp 100000 100 8; rm -rf $$tmp

bench-queue: src/bench_queue.c src/queue.c src/queue.h
	$(CC) $(CFLAGS) -Isrc -o bench_queue src/bench_queue.c src/queue.c $(LDFLAGS)
	@tmp=$$(mktemp -d); ./bench_queue $$tmp 20000 100; rm -rf $$tmp

bench-matrix: kuttidb kuttidb-bench
	@python3 src/bench_matrix.py

bench-exchange: kuttidb
	@python3 src/bench_exchange.py

bench-quick: kuttidb kuttidb-bench
	@python3 src/bench_matrix.py --quick

bench-single: kuttidb kuttidb-bench
	@set -e; ./kuttidb 7393 - 100 2>/dev/null & server_pid=$$!; \
	trap 'kill $$server_pid 2>/dev/null || true' EXIT; sleep 0.7; \
	./kuttidb-bench 7393 1 20000 1 100 single; \
	./kuttidb-bench 7393 4 20000 1 100 single; \
	kill $$server_pid; wait $$server_pid || true

sanitize: src/test_kuttidb_core.c src/kuttidb.c src/embed.c src/embed_kuttidb.c src/kuttidb.h src/kuttidb_int.h src/embed_int.h
	$(CC) $(CFLAGS) -O1 -g -Isrc \
		-fsanitize=address,undefined -fno-omit-frame-pointer \
		-o core_test_sanitize src/test_kuttidb_core.c src/kuttidb.c src/embed.c src/embed_kuttidb.c
	@ASAN_OPTIONS=detect_leaks=0 ./core_test_sanitize

sanitize-stream: src/test_stream.c src/stream.c src/stream.h
	$(CC) $(CFLAGS) -O1 -g -Isrc \
		-fsanitize=address,undefined -fno-omit-frame-pointer \
		-o stream_test_sanitize src/test_stream.c src/stream.c
	@ASAN_OPTIONS=detect_leaks=0 ./stream_test_sanitize

sanitize-fuzz: src/test_fuzz.c src/stream.c src/queue.c src/stream.h src/queue.h
	$(CC) $(CFLAGS) -O1 -g -Isrc \
		-fsanitize=address,undefined -fno-omit-frame-pointer \
		-o fuzz_test_sanitize src/test_fuzz.c src/stream.c src/queue.c
	@ASAN_OPTIONS=detect_leaks=0 ./fuzz_test_sanitize

sanitize-tsan-queue: src/test_queue_failures.c src/queue.c src/queue.h
	$(CC) $(CFLAGS) -O1 -g -Isrc \
		-fsanitize=thread -fno-omit-frame-pointer \
		-o queue_tsan_test src/test_queue_failures.c src/queue.c
	@TSAN_OPTIONS=halt_on_error=1 ./queue_tsan_test

sanitize-tsan-server: src/server.c src/admin_http.c src/admin_json.c src/kuttidb.c src/embed.c src/embed_kuttidb.c src/platform.c src/queue.c src/stream.c src/instance_lock.c src/managed_lifecycle.c src/managed_launcher.c
	$(CC) $(CFLAGS) -O1 -g -Isrc \
		-fsanitize=thread -fno-omit-frame-pointer \
		-o kuttidb_tsan src/server.c src/admin_http.c src/admin_json.c src/kuttidb.c src/embed.c src/embed_kuttidb.c src/platform.c src/queue.c src/stream.c src/instance_lock.c src/managed_lifecycle.c src/managed_launcher.c $(TLS_LIBS)
	@KUTTIDB_SERVER=./kuttidb_tsan python3 src/test_queue_protocol.py
	@KUTTIDB_SERVER=./kuttidb_tsan python3 src/test_stampede_protocol.py
	@KUTTIDB_SERVER=./kuttidb_tsan python3 src/test_stream_protocol.py

sanitize-server: src/server.c src/admin_http.c src/admin_json.c src/kuttidb.c src/embed.c src/embed_kuttidb.c src/platform.c src/queue.c src/stream.c src/instance_lock.c src/managed_lifecycle.c src/managed_launcher.c
	$(CC) $(CFLAGS) -O1 -g -Isrc \
		-fsanitize=address,undefined -fno-omit-frame-pointer \
		-o kuttidb_sanitize src/server.c src/admin_http.c src/admin_json.c src/kuttidb.c src/embed.c src/embed_kuttidb.c src/platform.c src/queue.c src/stream.c src/instance_lock.c src/managed_lifecycle.c src/managed_launcher.c
	@ASAN_OPTIONS=detect_leaks=0 KUTTIDB_SERVER=./kuttidb_sanitize \
		python3 src/test_stream_protocol.py
	@ASAN_OPTIONS=detect_leaks=0 KUTTIDB_SERVER=./kuttidb_sanitize \
		python3 src/test_management_api.py

.PHONY: all clean test managed-sdk-test bench bench-matrix bench-quick bench-single bench-exchange bench-stream bench-queue sanitize sanitize-stream sanitize-fuzz sanitize-tsan-queue sanitize-tsan-server sanitize-server
