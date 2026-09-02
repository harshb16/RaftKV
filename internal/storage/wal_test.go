package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRecoveryTailAndCompaction(t *testing.T) {
	d := t.TempDir()
	w, e := Open(d)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = Open(d); e == nil {
		t.Fatal("double open")
	}
	for _, b := range []string{"term=1", "term=2"} {
		if e = w.Save([]byte(b)); e != nil {
			t.Fatal(e)
		}
	}
	_ = w.Close()
	f, e := os.OpenFile(filepath.Join(d, "raft.wal"), os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	_, _ = f.Write([]byte("RKV2\x00"))
	_ = f.Close()
	w, e = Open(d)
	if e != nil {
		t.Fatal(e)
	}
	b, _ := w.Load()
	if string(b) != "term=2" {
		t.Fatal(string(b))
	}
	if e = w.Compact([]byte("snapshot")); e != nil {
		t.Fatal(e)
	}
	_ = w.Close()
	w, e = Open(d)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Close()
	b, _ = w.Load()
	if string(b) != "snapshot" {
		t.Fatal(string(b))
	}
}
func TestCorruptionFailsClosed(t *testing.T) {
	d := t.TempDir()
	w, e := Open(d)
	if e != nil {
		t.Fatal(e)
	}
	_ = w.Save([]byte("durable"))
	_ = w.Close()
	path := filepath.Join(d, "raft.wal")
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	b[len(b)-1] ^= 1
	_ = os.WriteFile(path, b, 0600)
	if _, e = Open(d); e == nil {
		t.Fatal("accepted corrupt record")
	}
}
func TestEveryTornPayloadPrefix(t *testing.T) {
	for cut := 1; cut < 40; cut++ {
		d := t.TempDir()
		w, e := Open(d)
		if e != nil {
			t.Fatal(e)
		}
		_ = w.Save([]byte("safe"))
		_ = w.Close()
		r, _ := record(make([]byte, 40))
		f, _ := os.OpenFile(filepath.Join(d, "raft.wal"), os.O_APPEND|os.O_WRONLY, 0600)
		_, _ = f.Write(r[:cut])
		_ = f.Close()
		w, e = Open(d)
		if e != nil {
			t.Fatal(cut, e)
		}
		b, _ := w.Load()
		_ = w.Close()
		if string(b) != "safe" {
			t.Fatal(cut, string(b))
		}
	}
}

func TestHeaderCorruptionCannotMasqueradeAsTornTail(t *testing.T) {
	for i := 0; i < 16; i++ {
		d := t.TempDir()
		w, e := Open(d)
		if e != nil {
			t.Fatal(e)
		}
		if e = w.Save([]byte("acknowledged")); e != nil {
			t.Fatal(e)
		}
		_ = w.Close()
		p := filepath.Join(d, "raft.wal")
		b, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		b[i] ^= 1
		if e = os.WriteFile(p, b, 0600); e != nil {
			t.Fatal(e)
		}
		if w, e = Open(d); e == nil {
			_ = w.Close()
			t.Fatalf("accepted corrupted header byte %d", i)
		}
	}
}
