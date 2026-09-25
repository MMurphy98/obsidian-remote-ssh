package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/sotashimozono/obsidian-remote-ssh/server/internal/correlator"
	"github.com/sotashimozono/obsidian-remote-ssh/server/internal/proto"
	"github.com/sotashimozono/obsidian-remote-ssh/server/internal/rpc"
	"github.com/sotashimozono/obsidian-remote-ssh/server/internal/vaultfs"
)

// registerCidIfPresent threads the optional Phase C correlation id
// from the inbound RPC envelope (request `meta.cid`) into the
// per-daemon Correlator, keyed by `paths`. Multi-path callers (e.g.
// fs.rename) register the cid against both the source and destination
// so whichever fsnotify event fires first carries it.
//
// All branches are short-circuited when the daemon was built without
// a correlator (production may run with cid threading disabled) or
// when no meta was on the wire — handlers stay one-line.
func registerCidIfPresent(ctx context.Context, cor *correlator.Correlator, paths ...string) {
	if cor == nil {
		return
	}
	meta, ok := rpc.MetaFromContext(ctx)
	if !ok || meta == nil || meta.Cid == "" {
		return
	}
	for _, p := range paths {
		cor.Register(p, meta.Cid)
	}
}

// decodeParams unmarshals raw into out and returns an InvalidParams
// rpc.Error on failure. `methodName` is used to tag the error so the
// client can correlate it with the call that failed.
func decodeParams(methodName string, raw json.RawMessage, out interface{}) *rpc.Error {
	if len(raw) == 0 || string(raw) == "null" {
		// Methods with no params accept this; methods that need params
		// will notice the zero value downstream.
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return rpc.ErrInvalidParams(methodName + ": " + err.Error())
	}
	return nil
}

// resolveOrErr wraps vaultfs.Resolve with a PathOutsideVault response.
func resolveOrErr(vaultRoot, relative string) (string, *rpc.Error) {
	abs, err := vaultfs.Resolve(vaultRoot, relative)
	if err != nil {
		return "", rpc.ErrPathOutsideVault(relative)
	}
	return abs, nil
}

// mapFsError maps a filesystem error to the nearest proto error code.
// `relativePath` is echoed in the message so the client sees a vault-
// relative path, not the absolute path on the remote.
func mapFsError(err error, relativePath string) *rpc.Error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return rpc.ErrFileNotFound(relativePath)
	case errors.Is(err, fs.ErrPermission):
		return rpc.ErrPermissionDenied(relativePath)
	case errors.Is(err, fs.ErrExist):
		return rpc.ErrExists(relativePath)
	case isNotDirectoryErr(err):
		return rpc.ErrNotADirectory(relativePath)
	case isIsDirectoryErr(err):
		return rpc.ErrIsADirectory(relativePath)
	}
	return rpc.ErrInternal(err.Error())
}

// entryTypeFrom maps an os.FileInfo / fs.DirEntry type to the proto
// EntryType string the client speaks. Symlinks are reported as
// symlinks (not followed); the caller decides whether to stat the
// target separately.
func entryTypeFrom(mode os.FileMode) proto.EntryType {
	switch {
	case mode&os.ModeSymlink != 0:
		return proto.EntryTypeSymlink
	case mode.IsDir():
		return proto.EntryTypeFolder
	default:
		return proto.EntryTypeFile
	}
}

// mtimeMillis renders a file mtime as unix milliseconds, matching the
// wire format used by proto.Stat.Mtime and proto.Entry.Mtime.
func mtimeMillis(info os.FileInfo) int64 {
	return info.ModTime().UnixMilli()
}

// isNotDirectoryErr / isIsDirectoryErr detect the POSIX-style
// directory-related errors (ENOTDIR, EISDIR) across platforms. Go's
// standard library doesn't expose these as sentinel errors, so we
// unwrap to the syscall.Errno layer manually. On Windows these errors
// are reported with different codes; the best-effort fallthrough is
// harmless (mapFsError returns InternalError in that case).

func isNotDirectoryErr(err error) bool {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno == syscall.ENOTDIR
	}
	return false
}

func isIsDirectoryErr(err error) bool {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno == syscall.EISDIR
	}
	return false
}

// writeFilePerm is the mode used for newly-written files. Matches the
// default Obsidian produces on desktop (umask-applied 0644).
const writeFilePerm = 0o644

// checkWritePrecondition distinguishes create-only (zero), update-only
// (non-zero), and explicitly unconditional (nil) writes. Missing files are
// conflicts for old revisions: replay must not undo a concurrent deletion.
func checkWritePrecondition(abs, relativePath string, expectedMtime *int64) (os.FileInfo, *rpc.Error) {
	info, err := os.Stat(abs)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, mapFsError(err, relativePath)
	}
	if expectedMtime != nil {
		if *expectedMtime == 0 {
			if info != nil {
				return nil, rpc.ErrPreconditionFailed(relativePath + ": expected a missing file, found an existing target")
			}
		} else if info == nil {
			return nil, rpc.ErrPreconditionFailed(relativePath + ": expected an existing file, but it was deleted or moved")
		} else if got := mtimeMillis(info); got != *expectedMtime {
			return nil, rpc.ErrPreconditionFailed(fmt.Sprintf("%s: expected mtime %d, found %d", relativePath, *expectedMtime, got))
		}
	}
	return info, nil
}

// advanceMtime prevents daemon writes within the same millisecond (or after
// a clock adjustment) from reusing the previous optimistic concurrency token.
// External writers can still preserve/restore mtimes; full protection from
// those writers requires a stronger revision protocol and cooperation.
func advanceMtime(abs string, previous os.FileInfo) error {
	if previous == nil {
		return nil
	}
	info, err := os.Stat(abs)
	if err != nil {
		return err
	}
	if mtimeMillis(info) > mtimeMillis(previous) {
		return nil
	}
	next := time.UnixMilli(mtimeMillis(previous) + 1)
	if err := os.Chtimes(abs, time.Now(), next); err != nil {
		return err
	}
	info, err = os.Stat(abs)
	if err != nil {
		return err
	}
	if mtimeMillis(info) <= mtimeMillis(previous) {
		return fmt.Errorf("filesystem cannot advance the write revision in milliseconds")
	}
	return nil
}

// atomicWriteFile stages a complete file and then publishes it atomically.
// Its caller must hold the vault mutation lock through the whole operation.
// expectedMtime=nil is unconditional, zero is create-only, and non-zero
// requires an existing matching revision. Return the staged inode's mtime,
// never a post-rename path stat that could describe someone else's write.
// wasModify controls the synthetic watcher notification (#108).
func atomicWriteFile(abs, relativePath string, data []byte, expectedMtime *int64) (int64, bool, *rpc.Error) {
	previous, e := checkWritePrecondition(abs, relativePath, expectedMtime)
	if e != nil {
		return 0, false, e
	}

	parent := filepath.Dir(abs)
	if err := os.MkdirAll(parent, 0o755); err != nil { // #nosec G301 — vault directory
		return 0, false, mapFsError(err, relativePath)
	}
	tmp, err := os.CreateTemp(parent, ".rsh-write-*.tmp")
	if err != nil {
		return 0, false, mapFsError(err, relativePath)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	defer tmp.Close()
	if _, err := tmp.Write(data); err != nil {
		return 0, false, mapFsError(err, relativePath)
	}
	if err := tmp.Chmod(writeFilePerm); err != nil {
		return 0, false, mapFsError(err, relativePath)
	}
	if err := advanceMtime(tmpPath, previous); err != nil {
		return 0, false, mapFsError(err, relativePath)
	}
	if err := tmp.Sync(); err != nil {
		return 0, false, mapFsError(err, relativePath)
	}
	committed, err := tmp.Stat()
	if err != nil {
		return 0, false, mapFsError(err, relativePath)
	}
	if err := tmp.Close(); err != nil {
		return 0, false, mapFsError(err, relativePath)
	}

	// Recheck after staging to detect non-participating editors during the
	// potentially long write. Advisory locking closes the window between
	// participating daemons; external writes can still race this final check.
	current, e := checkWritePrecondition(abs, relativePath, expectedMtime)
	if e != nil {
		return 0, false, e
	}
	if expectedMtime != nil && previous != nil &&
		(!os.SameFile(previous, current) || !previous.ModTime().Equal(current.ModTime()) || previous.Size() != current.Size()) {
		return 0, false, rpc.ErrPreconditionFailed(relativePath + ": file changed while preparing the write")
	}
	if expectedMtime != nil && *expectedMtime == 0 {
		// Link publishes the staged inode only if the target is still absent.
		// Unlike rename, this cannot overwrite even a non-cooperating creator.
		if err := os.Link(tmpPath, abs); err != nil {
			if errors.Is(err, fs.ErrExist) {
				return 0, false, rpc.ErrPreconditionFailed(relativePath + ": another writer created the file")
			}
			return 0, false, mapFsError(err, relativePath)
		}
	} else if err := os.Rename(tmpPath, abs); err != nil {
		return 0, false, mapFsError(err, relativePath)
	}
	return mtimeMillis(committed), previous != nil, nil
}
