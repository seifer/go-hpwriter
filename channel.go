package hpwriter

import (
	"io"
	"time"
)

const (
	LOG_BUF_SIZE   = 2048
	WRITE_ATTEMPTS = 3
)

type Channel struct {
	w io.Writer
	c chan []byte
}

func NewThroughChannel(w io.Writer) *Channel {
	ww := &Channel{
		w: w,
		c: make(chan []byte, 2048),
	}

	go ww.writer()

	return ww
}

func (w *Channel) Write(buf []byte) (int, error) {
	w.c <- buf
	return len(buf), nil
}

func (w *Channel) writer() {
	var err error
	var flush bool
	var n, nn, att int
	var buf, msg []byte

	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-ticker.C:
			flush = true
		case msg = <-w.c:
			buf = append(buf, msg...)
			flush = len(buf) >= LOG_BUF_SIZE
		}

		if flush {
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
}
