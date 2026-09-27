package remotestore

import (
	"context"
	"strconv"
	"strings"
)

// ensureContext lets the adapters bail out between protocol round-trips; the
// byte-level cancellation lives in the rate limited reader.
func ensureContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func itoa(v int) string { return strconv.Itoa(v) }

// splitPath 拆出路径的每一级，并保留是否为绝对路径这一信息（首元素为空串）。
func splitPath(p string) []string {
	return strings.Split(strings.Trim(p, "\\"), "/")
}

func isAbs(p string) bool { return strings.HasPrefix(p, "/") }
