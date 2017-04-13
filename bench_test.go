package hpwriter

import (
	"io"
	"os"
	"sync"
	"testing"
)

type Locked struct {
	w io.Writer
	m sync.Mutex
}

func NewLocked(w io.Writer) *Locked {
	return &Locked{w: w}
}

func (w *Locked) Write(buf []byte) (n int, err error) {
	w.m.Lock()
	n, err = w.w.Write(buf)
	w.m.Unlock()

	return
}

var w = os.Stderr
var m = []byte("test\n")
var wLock = NewLocked(w)
var wChannel = NewThroughChannel(w)
var wShardedBuffers = NewThroughShardedBuffers(w, 32)

func BenchmarkLock(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wLock.Write(m)
		}
	})
}

func BenchmarkChan(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wChannel.Write(m)
		}
	})
}

func BenchmarkShard(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wShardedBuffers.Write(m)
		}
	})
}
