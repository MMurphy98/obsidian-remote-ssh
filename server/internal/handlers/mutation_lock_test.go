//go:build linux || darwin

package handlers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/sotashimozono/obsidian-remote-ssh/server/internal/proto"
	"github.com/sotashimozono/obsidian-remote-ssh/server/internal/rpc"
)

// A subprocess, rather than two goroutines, catches accidental use of an
// in-memory mutex: separate Obsidian clients can start separate daemons.
func TestConditionalWrite_RespectsOtherProcessMutation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "note.md")
	if err := os.WriteFile(path, []byte("base"), 0o644); err != nil {
		t.Fatal(err)
	}
	oldTime := time.Unix(1_700_000_000, 0)
	if err := os.Chtimes(path, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	lock, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)

	cmd := exec.Command(os.Args[0], "-test.run=^TestConditionalWriteProcess$")
	cmd.Env = append(os.Environ(), "REMOTE_SSH_WRITE_TEST_ROOT="+root, "REMOTE_SSH_WRITE_TEST_MTIME="+strconv.FormatInt(oldTime.UnixMilli(), 10))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "ready" {
		t.Fatalf("child did not start: %q", scanner.Text())
	}
	result := make(chan string, 1)
	go func() {
		if scanner.Scan() {
			result <- scanner.Text()
		} else {
			result <- "no response"
		}
	}()
	select {
	case got := <-result:
		t.Fatalf("write bypassed another process's vault mutation lock: %s", got)
	case <-time.After(100 * time.Millisecond):
	}
	// Commit another edit while the child's stale write waits for our lock.
	if err := os.WriteFile(path, []byte("other client"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got != strconv.Itoa(proto.ErrorPreconditionFailed) {
			t.Fatalf("stale subprocess write result = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("child remained blocked after releasing vault lock")
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "other client" {
		t.Fatalf("concurrent edit lost: %q, %v", got, err)
	}
}

func TestConditionalWriteProcess(t *testing.T) {
	root := os.Getenv("REMOTE_SSH_WRITE_TEST_ROOT")
	if root == "" {
		t.Skip("subprocess helper")
	}
	mtime, err := strconv.ParseInt(os.Getenv("REMOTE_SSH_WRITE_TEST_MTIME"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(proto.WriteTextParams{Path: "note.md", Content: "stale offline edit", ExpectedMtime: writeMtime(mtime)})
	fmt.Fprintln(os.Stdout, "ready")
	_, e := FsWrite(root, nil, nil)(context.Background(), raw)
	code := 0
	if e != nil {
		code = e.Code
	}
	fmt.Fprintln(os.Stdout, code)
}

func TestMutationLock_CoversAllMutationHandlers(t *testing.T) {
	for _, method := range []string{"write", "writeBinary", "append", "appendBinary", "copy", "rename", "remove", "rmdir", "trashLocal", "mkdir"} {
		t.Run(method, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "note.md"), []byte("base"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(root, "folder"), 0o755); err != nil {
				t.Fatal(err)
			}
			lock, err := os.Open(root)
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Close()
			if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX); err != nil {
				t.Fatal(err)
			}
			var h rpc.Handler
			var raw json.RawMessage
			switch method {
			case "write":
				h, raw = FsWrite(root, nil, nil), json.RawMessage(`{"path":"note.md","content":"new"}`)
			case "writeBinary":
				h, raw = FsWriteBinary(root, nil, nil), json.RawMessage(`{"path":"note.md","contentBase64":"bmV3"}`)
			case "append":
				h, raw = FsAppend(root), json.RawMessage(`{"path":"note.md","content":"new"}`)
			case "appendBinary":
				h, raw = FsAppendBinary(root), json.RawMessage(`{"path":"note.md","contentBase64":"bmV3"}`)
			case "copy":
				h, raw = FsCopy(root, nil), json.RawMessage(`{"srcPath":"note.md","destPath":"copy.md"}`)
			case "rename":
				h, raw = FsRename(root, nil), json.RawMessage(`{"oldPath":"note.md","newPath":"moved.md"}`)
			case "remove":
				h, raw = FsRemove(root, nil), json.RawMessage(`{"path":"note.md"}`)
			case "rmdir":
				h, raw = FsRmdir(root), json.RawMessage(`{"path":"folder","recursive":true}`)
			case "trashLocal":
				h, raw = FsTrashLocal(root), json.RawMessage(`{"path":"note.md"}`)
			case "mkdir":
				h, raw = FsMkdir(root), json.RawMessage(`{"path":"new-folder"}`)
			}
			started := make(chan struct{})
			result := make(chan *rpc.Error, 1)
			go func() { close(started); _, e := h(context.Background(), raw); result <- e }()
			<-started
			select {
			case e := <-result:
				t.Fatalf("mutation bypassed the vault lock: %+v", e)
			case <-time.After(30 * time.Millisecond):
			}
			if err := unix.Flock(int(lock.Fd()), unix.LOCK_UN); err != nil {
				t.Fatal(err)
			}
			select {
			case e := <-result:
				if e != nil {
					t.Fatalf("mutation failed after unlock: %+v", e)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("mutation did not resume after unlock")
			}
		})
	}
}
