//go:build !linux && !darwin

package handlers

import (
	"context"
	"encoding/json"

	"github.com/sotashimozono/obsidian-remote-ssh/server/internal/rpc"
)

// The daemon is distributed for Linux and macOS. Fail closed if built for
// another platform until it has an equivalent cross-process vault lock.
func withMutationLock(_ string, _ rpc.Handler) rpc.Handler {
	return func(context.Context, json.RawMessage) (interface{}, *rpc.Error) {
		return nil, rpc.ErrInternal("vault mutation locking is unsupported on this platform")
	}
}
