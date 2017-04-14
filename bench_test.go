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

var m = []byte("test\n")
var w, wLock, wChannel, wShardedBuffers io.Writer

func init() {
	var err1 error

	w, err1 = os.OpenFile("/dev/null", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0755)
	if err1 != nil {
		panic(err1)
	}

	file1, err := os.OpenFile("/dev/null", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	wLock = NewLocked(file1)

	file2, err := os.OpenFile("/dev/null", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	wChannel = NewThroughChannel(file2)

	file3, err := os.OpenFile("/dev/null", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	wShardedBuffers = NewThroughShardedBuffers(file3, 32)
}

func BenchmarkFree(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w.Write(m)
		}
	})
}

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
