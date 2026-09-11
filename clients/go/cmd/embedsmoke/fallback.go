//go:build !kuttidb_embed

package main

// Fallback for every build without the explicit kuttidb_embed tag: the
// embedding-only smoke never participates in default builds, and building
// without the tag must not silently substitute network-only behavior.

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "embedsmoke requires the explicit embedding build:")
	fmt.Fprintln(os.Stderr, "  CGO_ENABLED=1 go run -tags kuttidb_embed ./cmd/embedsmoke [region] [port]")
	fmt.Fprintln(os.Stderr, "(embedding needs CGO_ENABLED=1, -tags kuttidb_embed, a C toolchain,")
	fmt.Fprintln(os.Stderr, " and the libkuttidb_embed shared library built by `make`.)")
	os.Exit(2)
}