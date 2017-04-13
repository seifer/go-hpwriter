package hpwriter

import (
	"io"
	"sync"
	"sync/atomic"
)

type ShardedBuffers struct {
	w io.Writer
	c chan int64

	cs int64
	sc int64
	ab [][]byte
	al []sync.Mutex
}

func NewThroughShardedBuffers(w io.Writer, sc int64) *ShardedBuffers {
	ww := &ShardedBuffers{
		w: w,
		c: make(chan int64, sc),

		sc: sc,
		ab: make([][]byte, sc),
		al: make([]sync.Mutex, sc),
	}

	go ww.writer()

	return ww
}

func (w *ShardedBuffers) Write(buf []byte) (int, error) {
	cn := atomic.AddInt64(&w.cs, 1) & (w.sc - 1)

	w.al[cn].Lock()
	empty := len(w.ab[cn]) == 0
	w.ab[cn] = append(w.ab[cn], buf...)
	w.al[cn].Unlock()

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

		w.al[nc].Lock()
		buf, w.ab[nc] = w.ab[nc], buf
		w.al[nc].Unlock()

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
