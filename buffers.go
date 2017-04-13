package hpwriter

import (
	"io"
	"sync"
	"sync/atomic"
)

type shard struct {
	sync.Mutex
	b    []byte
	_pad [12]uintptr
}

type ShardedBuffers struct {
	w io.Writer
	c chan uint32

	cs uint32
	sc uint32
	sh []shard
}

func NewThroughShardedBuffers(w io.Writer, sc int) *ShardedBuffers {
	if sc&(sc-1) != 0 {
		panic("sc (shards count) should be power of 2")
	}
	if sc <= 0 || sc > 4096 {
		panic("sc should be > 0 and <= 4096")
	}
	ww := &ShardedBuffers{
		w: w,
		c: make(chan uint32, sc),

		sc: uint32(sc),
		sh: make([]shard, sc),
	}

	go ww.writer()

	return ww
}

func (w *ShardedBuffers) Write(buf []byte) (int, error) {
	cn := atomic.AddUint32(&w.cs, 1) & (w.sc - 1)

	sh := &w.sh[cn]
	sh.Lock()
	empty := len(sh.b) == 0
	sh.b = append(sh.b, buf...)
	sh.Unlock()

	if empty {
		w.c <- cn
	}

	return len(buf), nil
}

func (w *ShardedBuffers) writer() {
	var err error
	var buf []byte
	var n, nn, att int

	for {
		nc := <-w.c
		sh := &w.sh[nc]

		sh.Lock()
		buf, sh.b = sh.b, buf
		sh.Unlock()

		n = 0
		nn = 0
		att = 0
		err = nil

		for n < len(buf) {
			nn, err = w.w.Write(buf[n:])
			n += nn

			if err != nil {
				if att > WRITE_ATTEMPTS {
					break
				}

				if err != io.ErrShortWrite {
					break
				}

				att++
			}
		}

		buf = buf[:0]
	}
}
