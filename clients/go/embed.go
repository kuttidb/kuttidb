//go:build cgo && kuttidb_embed

package kuttidb

// Shared-memory embedded mode: attaches to the region the server created
// and reads/writes records directly in memory - no socket, no wire protocol.
//
// Embedded mode is explicitly opt-in and needs all of the following:
//
//	CGO_ENABLED=1                     (a working C toolchain)
//	-tags kuttidb_embed               (build tag; without it this file and
//	                                   every OpenEmbed/EmbedDB symbol are
//	                                   excluded and the network package
//	                                   builds with no KuttiDB C dependency)
//	an explicit C development environment
//
// Build from a full repository checkout: the #cgo directives below look for
// the embed headers in <checkout>/src and the shared library
// libkuttidb_embed.{dylib,so} in <checkout> (both produced by `make`), with
// an rpath so the built binary runs from the checkout. To link an installed
// library instead, keep the checkout for the header (or add its include dir
// through CGO_CPPFLAGS) and point the linker at the installed library with
// CGO_LDFLAGS, e.g.:
//
//	CGO_ENABLED=1 CGO_LDFLAGS="-L/usr/local/lib -lkuttidb_embed" \
//	  go build -tags kuttidb_embed ./...
//
// The network package (kuttidb.New and friends) never needs either setting.
//
//	db, err := OpenEmbed("/tmp/db.embed")
//	db.Put("k", []byte("v"), 0)
//	v, _ := db.Get("k")
//
// All methods are safe for concurrent use by multiple goroutines (the C
// engine is sharded and thread-safe). Keys must not contain NUL bytes.

/*
#cgo CFLAGS: -I${SRCDIR}/../../src
#cgo LDFLAGS: -L${SRCDIR}/../.. -lkuttidb_embed -Wl,-rpath,${SRCDIR}/../..
#include <stdlib.h>
#include "embed.h"
*/
import "C"

import (
	"fmt"
	"time"
	"unsafe"
)

// EmbedDB is a handle to a shared-memory cache region.
//
// Durable work (atomic job completion) is NOT available through shared
// memory: the embed ABI is cache-only and has no queue delivery/commit
// coordinator. Open a normal network Client (kuttidb.New / NewManaged)
// pointed at the same server instance for job_consume/job_complete/state_*
// operations — never attempt to synthesize a completion by writing through
// the region and sending a separate ACK. Verify the instance identity when
// combining both paths in one process (ServerInfo) so cache and durable
// work cannot end up on different servers.
type EmbedDB struct {
	ec *C.KuttiEmbed
	c  *C.KuttiDB
}

// OpenEmbed attaches to an existing region file (the server creates it).
func OpenEmbed(path string) (*EmbedDB, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ec := C.kuttidb_embed_open(cpath)
	if ec == nil {
		return nil, fmt.Errorf("cannot attach embed region %s", path)
	}
	return &EmbedDB{ec: ec, c: C.kuttidb_embed_cache(ec)}, nil
}

// Put stores value under key; ttl 0 = no expiry.
func (d *EmbedDB) Put(key string, value []byte, ttl time.Duration) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))
	var cval *C.char
	if len(value) > 0 {
		cval = (*C.char)(unsafe.Pointer(&value[0]))
	}
	ttlMs := C.uint64_t(ttl.Milliseconds())
	if ttl > 0 && ttlMs == 0 {
		ttlMs = 1
	}
	if rc := C.kuttidb_embed_put(d.ec, ckey, C.uint32_t(len(key)), cval, C.uint32_t(len(value)), ttlMs); rc != 0 {
		return fmt.Errorf("embed put failed: %d", int(rc))
	}
	return nil
}

// Get returns the value for key, or (nil, false) on miss.
func (d *EmbedDB) Get(key string) ([]byte, bool, error) {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))
	var out *C.char
	var outlen C.uint32_t
	rc := C.kuttidb_get(d.c, ckey, C.uint32_t(len(key)), (**C.char)(unsafe.Pointer(&out)), &outlen)
	switch rc {
	case 1:
		defer C.kuttidb_free_value(unsafe.Pointer(out))
		return C.GoBytes(unsafe.Pointer(out), C.int(outlen)), true, nil
	case 0:
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("embed get error: %d", int(rc))
	}
}

// Delete reports whether the key existed.
func (d *EmbedDB) Delete(key string) (bool, error) {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))
	rc := C.kuttidb_embed_delete(d.ec, ckey, C.uint32_t(len(key)))
	return rc == 1, nil
}

// Count returns the number of live records.
func (d *EmbedDB) Count() uint64 { return uint64(C.kuttidb_count(d.c)) }

// MemUsage returns live memory used by records and tables.
func (d *EmbedDB) MemUsage() uint64 { return uint64(C.kuttidb_memusage(d.c)) }

// Close detaches from the region (the region itself persists).
func (d *EmbedDB) Close() {
	if d.ec != nil {
		C.kuttidb_embed_close(d.ec)
		d.ec = nil
	}
}
