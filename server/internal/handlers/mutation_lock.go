//go:build linux || darwin

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	"golang.org/x/sys/unix"

	"github.com/sotashimozono/obsidian-remote-ssh/server/internal/rpc"
)

// All daemon mutations must use the same lock, including directory moves
// and recursive deletes. A per-path or per-process mutex cannot protect a
// write's stat-to-rename interval from a second daemon serving this vault.
// Locking the root directory itself avoids a lock file that a client could
// rename/delete. Each invocation opens its own descriptor, so flock also
// serializes independent connections in the same process. Closing it drops
// the lock on every return path (including process termination).
//
// This is advisory: SFTP, local editors, and older daemons do not participate.
// Unsupported filesystems fail closed instead of silently skipping the lock.
func withMutationLock(vaultRoot string, h rpc.Handler) rpc.Handler {
	return func(ctx context.Context, params json.RawMessage) (interface{}, *rpc.Error) {
		root, err := os.Open(vaultRoot) // #nosec G304 — configured vault root
		if err != nil {
			return nil, mapFsError(err, "")
		}
		defer root.Close()
		for {
			err = unix.Flock(int(root.Fd()), unix.LOCK_EX|unix.LOCK_NB)
			if err == nil {
				break
			}
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
				return nil, rpc.ErrInternal("cannot lock vault for mutation: " + err.Error())
			}
			select {
			case <-ctx.Done():
				return nil, rpc.ErrInternal("waiting for vault mutation lock: " + ctx.Err().Error())
			case <-time.After(10 * time.Millisecond):
			}
		}
		return h(ctx, params)
	}
}
