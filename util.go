package yamux

import (
	"io"
	"sync"
	"sync/atomic"

	pool "github.com/libp2p/go-buffer-pool"
)

// asyncSendErr is used to try an async send of an error
func asyncSendErr(ch chan error, err error) {
	if ch == nil {
		return
	}
	select {
	case ch <- err:
	default:
	}
}

// asyncNotify is used to signal a waiting goroutine
func asyncNotify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// min computes the minimum of a set of values
func min(values ...uint32) uint32 {
	m := values[0]
	for _, v := range values[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

type segmentedBuffer struct {
	cap uint32
	len int32
	bm  sync.Mutex
	b   [][]byte
}

func NewSegmentedBuffer(initialCapacity uint32) segmentedBuffer {
	s := segmentedBuffer{b: make([][]byte, 0)}
	atomic.AddUint32(&s.cap, initialCapacity)
	return s
}

func (s *segmentedBuffer) Len() int {
	return int(atomic.LoadInt32(&s.len))
}

func (s *segmentedBuffer) Cap() uint32 {
	return atomic.LoadUint32(&s.cap)
}

func (s *segmentedBuffer) Grow(amt uint32) {
	atomic.AddUint32(&s.cap, amt)
}

func (s *segmentedBuffer) Read(b []byte) (int, error) {
	s.bm.Lock()
	defer s.bm.Unlock()
	if len(s.b) == 0 {
		return 0, io.EOF
	}
	n := copy(b, s.b[0])
	if n == len(s.b[0]) {
		pool.Put(s.b[0])
		s.b[0] = nil
		s.b = s.b[1:]
	} else {
		s.b[0] = s.b[0][n:]
	}
	atomic.AddInt32(&s.len, -1*int32(n))
	return n, nil
}

func (s *segmentedBuffer) Append(input io.Reader, length int) error {
	dst := pool.Get(length)
	n := 0
	for {
		read, err := input.Read(dst[n:])
		n += read
		switch err {
		case nil:
		case io.EOF:
			err = nil
			fallthrough
		default:
			s.bm.Lock()
			defer s.bm.Unlock()
			atomic.AddInt32(&s.len, int32(n))
			// cap -= n
			atomic.AddUint32(&s.cap, ^uint32(n-1))
			s.b = append(s.b, dst[0:n])
			return err
		}
	}
}
