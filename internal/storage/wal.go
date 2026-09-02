// Package storage stores self-contained Raft checkpoints in a checksummed WAL.
// Each acknowledgement follows fsync. Only an incomplete EOF tail is discarded;
// a complete record with an invalid checksum fails recovery closed.
package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const maxRecord = 256 << 20

var magic = [4]byte{'R', 'K', 'V', '2'}

type WAL struct {
	mu     sync.Mutex
	dir    string
	f      *os.File
	lock   *os.File
	last   []byte
	closed bool
}

func Open(dir string) (*WAL, error) {
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	lock, e := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("data directory already in use: %w", e)
	}
	f, e := os.OpenFile(filepath.Join(dir, "raft.wal"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		_ = lock.Close()
		return nil, e
	}
	w := &WAL{dir: dir, f: f, lock: lock}
	if e = w.recover(); e != nil {
		_ = w.Close()
		return nil, e
	}
	if e = syncDir(dir); e != nil {
		_ = w.Close()
		return nil, e
	}
	return w, nil
}
func (w *WAL) recover() error {
	var offset int64
	for {
		h := make([]byte, 16)
		n, e := io.ReadFull(w.f, h)
		if e == io.EOF && n == 0 {
			break
		}
		if e == io.ErrUnexpectedEOF {
			return w.truncate(offset)
		}
		if e != nil {
			return e
		}
		if crc32.ChecksumIEEE(h[:12]) != binary.BigEndian.Uint32(h[12:16]) {
			return fmt.Errorf("WAL header checksum mismatch at %d", offset)
		}
		if string(h[:4]) != string(magic[:]) {
			return fmt.Errorf("WAL bad magic at %d", offset)
		}
		size := binary.BigEndian.Uint32(h[4:8])
		if size > maxRecord {
			return fmt.Errorf("WAL invalid length at %d", offset)
		}
		b := make([]byte, int(size))
		_, e = io.ReadFull(w.f, b)
		if e == io.EOF || e == io.ErrUnexpectedEOF {
			return w.truncate(offset)
		}
		if e != nil {
			return e
		}
		if crc32.ChecksumIEEE(b) != binary.BigEndian.Uint32(h[8:12]) {
			return fmt.Errorf("WAL checksum mismatch at %d", offset)
		}
		w.last = b
		offset += 16 + int64(size)
	}
	_, e := w.f.Seek(0, io.SeekEnd)
	return e
}
func (w *WAL) truncate(offset int64) error {
	if e := w.f.Truncate(offset); e != nil {
		return e
	}
	if e := w.f.Sync(); e != nil {
		return e
	}
	_, e := w.f.Seek(offset, io.SeekStart)
	return e
}
func record(b []byte) ([]byte, error) {
	if len(b) > maxRecord {
		return nil, fmt.Errorf("WAL checkpoint exceeds %d bytes", maxRecord)
	}
	h := make([]byte, 16+len(b))
	copy(h, magic[:])
	binary.BigEndian.PutUint32(h[4:8], uint32(len(b)))
	binary.BigEndian.PutUint32(h[8:12], crc32.ChecksumIEEE(b))
	binary.BigEndian.PutUint32(h[12:16], crc32.ChecksumIEEE(h[:12]))
	copy(h[16:], b)
	return h, nil
}
func writeSync(f *os.File, b []byte) error {
	r, e := record(b)
	if e != nil {
		return e
	}
	if _, e = f.Write(r); e != nil {
		return e
	}
	return f.Sync()
}
func (w *WAL) Load() ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, os.ErrClosed
	}
	return append([]byte(nil), w.last...), nil
}
func (w *WAL) Save(b []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	if e := writeSync(w.f, b); e != nil {
		return e
	}
	w.last = append([]byte(nil), b...)
	return nil
}

// Compact writes snapshot, hard state and suffix together. A single atomic rename
// prevents a crash from pairing a new snapshot with an incompatible old log.
func (w *WAL) Compact(b []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	f, e := os.CreateTemp(w.dir, "checkpoint-*")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if e = writeSync(f, b); e != nil {
		_ = f.Close()
		return e
	}
	if e = os.Rename(name, filepath.Join(w.dir, "raft.wal")); e != nil {
		_ = f.Close()
		return e
	}
	old := w.f
	w.f = f
	_ = old.Close()
	w.last = append([]byte(nil), b...)
	return syncDir(w.dir)
}
func syncDir(dir string) error {
	f, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	e := w.f.Close()
	_ = syscall.Flock(int(w.lock.Fd()), syscall.LOCK_UN)
	_ = w.lock.Close()
	return e
}
