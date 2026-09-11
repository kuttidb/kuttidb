#!/usr/bin/env bash
# External consumer fixture for the Go client (see
# docs/operations/CLIENT_PUBLISHING.md and the kuttidb agent skill).
#
# Proves that a clean application — with no ancestor KuttiDB src/ directory,
# no built library, and no checkout-wide go.work or replace — imports,
# builds, and runs the network package with either CGO setting, and that the
# embedding file is excluded from the default build. The integration tests
# that build the C server stay in the checkout; this fixture strips them.
#
# Usage: clients/go/scripts/external-consumer-check.sh [repo-root]
# Optional environment:
#   KUTTIDB_SERVER   server binary used for the runtime round trip
#                    (default: <root>/kuttidb when present, else build-only)
#   SKIP_EMBED=1     skip the explicit embedding case (no library built)
set -euo pipefail

root=${1:-$(cd "$(dirname "$0")/../../.." && pwd)}
modsrc=$root/clients/go
root=$(cd "$root" && pwd)
server=${KUTTIDB_SERVER:-$root/kuttidb}

tmp=$(mktemp -d "${TMPDIR:-/tmp}/kuttidb-go-consumer.XXXXXX")
trap 'rm -rf "$tmp"; [ -n "${server_pid:-}" ] && kill "$server_pid" 2>/dev/null || true' EXIT

# --- module-only copy of clients/go (no tests, no src/, no library) ------
mkdir -p "$tmp/mod/cmd"
find "$modsrc" -maxdepth 1 -name '*.go' ! -name '*_test.go' -exec cp {} "$tmp/mod/" \;
cp "$modsrc/go.mod" "$tmp/mod/go.mod"
cp -R "$modsrc/cmd/." "$tmp/mod/cmd/"

# --- consumer module referencing only the copy ---------------------------
mkdir -p "$tmp/consumer/cgouse"
cat > "$tmp/consumer/go.mod" <<'EOF'
module consumer

go 1.22

require github.com/kuttidb/kuttidb/clients/go v0.0.0

replace github.com/kuttidb/kuttidb/clients/go => ../mod
EOF

cat > "$tmp/consumer/cgouse/use.go" <<'EOF'
//go:build cgo

// Package cgouse models unrelated CGO use in a host application: plain
// libc only, no KuttiDB dependency. The cgo build must really compile this
// file (go list selects use.go, not fallback.go).
package cgouse

/*
#include <stdlib.h>
*/
import "C"

// Scratch allocates and frees one byte through libc.
func Scratch() {
	p := C.malloc(1)
	C.free(p)
}
EOF

cat > "$tmp/consumer/cgouse/fallback.go" <<'EOF'
//go:build !cgo

package cgouse

// Scratch compiles the package when CGO is disabled.
func Scratch() {}
EOF

cat > "$tmp/consumer/main.go" <<'EOF'
package main

import (
	"fmt"
	"os"

	"github.com/kuttidb/kuttidb/clients/go"
	"consumer/cgouse"
)

func main() {
	addr := os.Getenv("KUTTIDB_CONSUMER_ADDR")
	if addr == "" {
		// Build-only mode: importing and referencing the package must not
		// need KuttiDB headers, libraries, or CGO.
		_ = kuttidb.BatchSize
		cgouse.Scratch()
		fmt.Println("consumer builds and imports (build-only)")
		return
	}
	c, err := kuttidb.New(addr, 2)
	if err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	defer c.Close()
	cgouse.Scratch()
	if err := c.Put("external-consumer", []byte("ok")); err != nil {
		fmt.Println("put:", err)
		os.Exit(1)
	}
	v, err := c.Get("external-consumer")
	if err != nil || string(v) != "ok" {
		fmt.Println("get:", string(v), err)
		os.Exit(1)
	}
	fmt.Println("consumer ok")
}
EOF

# The fixture must be free of injected KuttiDB include/linker settings and
# must not see a checkout-wide go.work or replace.
unset CGO_CFLAGS CGO_LDFLAGS CGO_CPPFLAGS CGO_CXXFLAGS || true
if env | grep -q '^CGO_.*FLAGS=.*kuttidb'; then
	echo "FAIL: CGO flags inject KuttiDB paths" >&2
	exit 1
fi

check_default() {
	local label=$1 cgo=$2
	cd "$tmp/consumer"
	env CGO_ENABLED=$cgo go build ./...
	env CGO_ENABLED=$cgo go vet ./...
	env CGO_ENABLED=$cgo go build -o /dev/null . 2>&1 | grep -i 'kuttidb_embed' \
		&& { echo "FAIL: $cgo build pulled in KuttiDB native linker flags" >&2; exit 1; } || true
	# Go's selected package files must exclude embed.go: no cgo files and no
	# KuttiDB headers or libraries in either mode.
	files=$(env CGO_ENABLED=$cgo go list -f '{{.GoFiles}} {{.CgoFiles}}' \
		github.com/kuttidb/kuttidb/clients/go)
	case "$files" in
	*embed.go*)
		echo "FAIL: CGO_ENABLED=$cgo selected embed.go ($files)" >&2
		exit 1
		;;
	esac
	echo "default package (CGO_ENABLED=$cgo): embed.go excluded — $files"
	# The unrelated CGO package must compile real cgo only when CGO is on.
	cgofiles=$(env CGO_ENABLED=$cgo go list -f '{{.GoFiles}} {{.CgoFiles}}' consumer/cgouse)
	case "$cgofiles" in
	*use.go*) ;;
	*) [ "$cgo" = 0 ] || { echo "FAIL: CGO_ENABLED=1 did not compile the cgo package: $cgofiles" >&2; exit 1; } ;;
	esac
	echo "consumer cgouse (CGO_ENABLED=$cgo): $cgofiles"
}

check_default no-cgo 0
check_default cgo 1

# --- explicit embedding needs the tag AND the provisioned C artifacts ----
if [ "${SKIP_EMBED:-0}" != 1 ] && [ -f "$root/src/embed.h" ]; then
	lib=""
	for cand in "$root/libkuttidb_embed.dylib" "$root/libkuttidb_embed.so"; do
		if [ -f "$cand" ]; then
			lib=$cand
			break
		fi
	done
	if [ -z "$lib" ]; then
		echo "SKIP explicit-embed fixture: libkuttidb_embed not built at $root"
	else
		# Model an installed library: header and shared object staged in a
		# prefix, linked through environment flags only.
		mkdir -p "$tmp/prefix/include" "$tmp/prefix/lib"
		cp "$root/src/embed.h" "$root/src/kuttidb.h" "$tmp/prefix/include/"
		cp "$lib" "$tmp/prefix/lib/"
		cd "$tmp/consumer"
		env CGO_ENABLED=1 \
			CGO_CPPFLAGS="-I$tmp/prefix/include" \
			CGO_LDFLAGS="-L$tmp/prefix/lib -lkuttidb_embed -Wl,-rpath,$tmp/prefix/lib" \
			go build -tags kuttidb_embed -o /dev/null ./...
		echo "explicit embed build with an installed-library prefix: ok"
		# Without the tag the same sources build as network-only.
		env CGO_ENABLED=1 go build -o /dev/null ./...
	fi
fi

# --- optional runtime round trip against a real server -------------------
if [ -x "$server" ]; then
	wal=$tmp/kuttidb.wal
	"$server" 7395 "$wal" 100 --queue-wal "$tmp/queue.wal" 2>/dev/null &
	server_pid=$!
	ready=0
	for _ in $(seq 1 100); do
		if (exec 3<>/dev/tcp/127.0.0.1/7395) 2>/dev/null; then
			exec 3>&- 3<&- || true
			ready=1
			break
		fi
		sleep 0.05
	done
	[ "$ready" = 1 ] || { echo "FAIL: fixture server did not start" >&2; exit 1; }
	cd "$tmp/consumer"
	for cgo in 0 1; do
		out=$(env CGO_ENABLED=$cgo KUTTIDB_CONSUMER_ADDR=127.0.0.1:7395 go run .)
		[ "$out" = "consumer ok" ] || { echo "FAIL: runtime CGO_ENABLED=$cgo: $out" >&2; exit 1; }
		echo "runtime round trip (CGO_ENABLED=$cgo): ok"
	done
else
	echo "SKIP runtime round trip: no server binary at $server"
fi

echo "EXTERNAL CONSUMER CHECKS PASSED"