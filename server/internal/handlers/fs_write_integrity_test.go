package handlers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sotashimozono/obsidian-remote-ssh/server/internal/proto"
	"github.com/sotashimozono/obsidian-remote-ssh/server/internal/rpc"
)

func TestConditionalWrite_DoesNotResurrectAfterMutation(t *testing.T) {
	for _, binary := range []bool{false, true} {
		for _, mutation := range []string{"remove", "rename", "rmdir", "trash"} {
			t.Run(mutation+map[bool]string{false: "/text", true: "/binary"}[binary], func(t *testing.T) {
				root := t.TempDir()
				if err := os.Mkdir(filepath.Join(root, "notes"), 0o755); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(root, "notes", "a.md")
				if err := os.WriteFile(path, []byte("original"), 0o644); err != nil {
					t.Fatal(err)
				}
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				var h rpc.Handler
				var params interface{}
				switch mutation {
				case "remove":
					h, params = FsRemove(root, nil), proto.PathOnlyParams{Path: "notes/a.md"}
				case "rename":
					h, params = FsRename(root, nil), proto.RenameParams{OldPath: "notes/a.md", NewPath: "moved.md"}
				case "rmdir":
					h, params = FsRmdir(root), proto.RmdirParams{Path: "notes", Recursive: true}
				case "trash":
					h, params = FsTrashLocal(root), proto.PathOnlyParams{Path: "notes/a.md"}
				}
				raw, _ := json.Marshal(params)
				if _, e := h(context.Background(), raw); e != nil {
					t.Fatal(e)
				}
				h = FsWrite(root, nil, nil)
				params = proto.WriteTextParams{Path: "notes/a.md", Content: "offline edit", ExpectedMtime: writeMtime(info.ModTime().UnixMilli())}
				if binary {
					h = FsWriteBinary(root, nil, nil)
					params = proto.WriteBinaryParams{Path: "notes/a.md", ContentBase64: base64.StdEncoding.EncodeToString([]byte("offline edit")), ExpectedMtime: writeMtime(info.ModTime().UnixMilli())}
				}
				raw, _ = json.Marshal(params)
				if _, e := h(context.Background(), raw); e == nil || e.Code != proto.ErrorPreconditionFailed {
					t.Fatalf("want PreconditionFailed, got %+v", e)
				}
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("stale write recreated the removed path: %v", err)
				}
			})
		}
	}
}

func TestConditionalWrite_OnlyOneConcurrentWriterAcceptsRevision(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "note.md")
	if err := os.WriteFile(path, []byte("base"), 0o644); err != nil {
		t.Fatal(err)
	}
	baseTime := time.Unix(1_700_000_000, 0)
	if err := os.Chtimes(path, baseTime, baseTime); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(proto.WriteTextParams{Path: "note.md", Content: strings.Repeat("x", 4<<20), ExpectedMtime: writeMtime(baseTime.UnixMilli())})
	const writers = 8
	start := make(chan struct{})
	results := make(chan *rpc.Error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Separate handler instances model separate connections.
			h := FsWrite(root, nil, nil)
			<-start
			_, e := h(context.Background(), raw)
			results <- e
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	successes := 0
	for e := range results {
		if e == nil {
			successes++
		} else if e.Code != proto.ErrorPreconditionFailed {
			t.Fatalf("unexpected error: %+v", e)
		}
	}
	if successes != 1 {
		t.Fatalf("%d writes accepted the same remote revision; want exactly one", successes)
	}
}

func TestConditionalWrite_AdvancesRevisionDespiteClockGoingBackwards(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "note.md")
	if err := os.WriteFile(path, []byte("base"), 0o644); err != nil {
		t.Fatal(err)
	}
	baseTime := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	if err := os.Chtimes(path, baseTime, baseTime); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(proto.WriteTextParams{Path: "note.md", Content: "next", ExpectedMtime: writeMtime(baseTime.UnixMilli())})
	result, e := FsWrite(root, nil, nil)(context.Background(), raw)
	if e != nil {
		t.Fatal(e)
	}
	if mtime := result.(proto.MtimeResult).Mtime; mtime <= baseTime.UnixMilli() {
		t.Fatalf("mtime did not advance beyond old revision: %d <= %d", mtime, baseTime.UnixMilli())
	}
	if _, e := FsWrite(root, nil, nil)(context.Background(), raw); e == nil || e.Code != proto.ErrorPreconditionFailed {
		t.Fatalf("old revision reused after write: %+v", e)
	}
}

func TestConditionalWrite_ZeroMtimeRequiresMissingTarget(t *testing.T) {
	for _, method := range []string{"text", "binary"} {
		t.Run(method, func(t *testing.T) {
			root := t.TempDir()
			h := FsWrite(root, nil, nil)
			raw := json.RawMessage(`{"path":"new.md","content":"first","expectedMtime":0}`)
			if method == "binary" {
				h = FsWriteBinary(root, nil, nil)
				raw = json.RawMessage(`{"path":"new.md","contentBase64":"Zmlyc3Q=","expectedMtime":0}`)
			}
			if _, e := h(context.Background(), raw); e != nil {
				t.Fatalf("create missing target: %+v", e)
			}
			if err := os.WriteFile(filepath.Join(root, "new.md"), []byte("another client's note"), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, e := h(context.Background(), raw); e == nil || e.Code != proto.ErrorPreconditionFailed {
				t.Fatalf("create-only write overwrote an existing target: %+v", e)
			}
			if got, err := os.ReadFile(filepath.Join(root, "new.md")); err != nil || string(got) != "another client's note" {
				t.Fatalf("existing content changed: %q, %v", got, err)
			}
		})
	}
}

func TestConditionalWrite_RejectsRevisionReplacedByRename(t *testing.T) {
	root := t.TempDir()
	mtime := time.Unix(1_700_000_000, 0)
	for name, content := range map[string]string{"src.md": "incoming", "dst.md": "old destination"} {
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	rename, _ := json.Marshal(proto.RenameParams{OldPath: "src.md", NewPath: "dst.md"})
	if _, e := FsRename(root, nil)(context.Background(), rename); e != nil {
		t.Fatal(e)
	}
	stale, _ := json.Marshal(proto.WriteTextParams{Path: "dst.md", Content: "stale destination edit", ExpectedMtime: writeMtime(mtime.UnixMilli())})
	if _, e := FsWrite(root, nil, nil)(context.Background(), stale); e == nil || e.Code != proto.ErrorPreconditionFailed {
		t.Fatalf("stale write accepted the replaced destination's revision: %+v", e)
	}
	if got, err := os.ReadFile(filepath.Join(root, "dst.md")); err != nil || string(got) != "incoming" {
		t.Fatalf("renamed content lost: %q, %v", got, err)
	}
}

func TestConditionalWrite_AppendAdvancesRevision(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "note.md")
	if err := os.WriteFile(path, []byte("base"), 0o644); err != nil {
		t.Fatal(err)
	}
	mtime := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(proto.AppendTextParams{Path: "note.md", Content: " appended"})
	result, e := FsAppend(root)(context.Background(), raw)
	if e != nil {
		t.Fatal(e)
	}
	if got := result.(proto.MtimeResult).Mtime; got <= mtime.UnixMilli() {
		t.Fatalf("append reused or went behind prior revision: %d <= %d", got, mtime.UnixMilli())
	}
}
