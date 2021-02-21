package yamux

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"
	"time"
)

func TestAsyncSendErr(t *testing.T) {
	ch := make(chan error)
	asyncSendErr(ch, ErrTimeout)
	select {
	case <-ch:
		t.Fatalf("should not get")
	default:
	}

	ch = make(chan error, 1)
	asyncSendErr(ch, ErrTimeout)
	select {
	case <-ch:
	default:
		t.Fatalf("should get")
	}
}

func TestAsyncNotify(t *testing.T) {
	ch := make(chan struct{})
	asyncNotify(ch)
	select {
	case <-ch:
		t.Fatalf("should not get")
	default:
	}

	ch = make(chan struct{}, 1)
	asyncNotify(ch)
	select {
	case <-ch:
	default:
		t.Fatalf("should get")
	}
}

func TestMin(t *testing.T) {
	if min(1, 2) != 1 {
		t.Fatalf("bad")
	}
	if min(2, 1) != 1 {
		t.Fatalf("bad")
	}
}

func TestSegmentedBuffer(t *testing.T) {
	buf := newSegmentedBuffer(100, 100, func() time.Duration { return 0 })
	assert := func(len, cap uint32) {
		if buf.Len() != len {
			t.Fatalf("expected length %d, got %d", len, buf.Len())
		}
		buf.bm.Lock()
		defer buf.bm.Unlock()
		if buf.cap != cap {
			t.Fatalf("expected length %d, got %d", len, buf.Len())
		}
	}
	assert(0, 100)
	if err := buf.Append(bytes.NewReader([]byte("fooo")), 3); err != nil {
		t.Fatal(err)
	}
	assert(3, 97)

	out := make([]byte, 2)
	n, err := io.ReadFull(&buf, out)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected to read 2 bytes, read %d", n)
	}
	assert(1, 97)
	if grew, amount := buf.GrowTo(false, time.Now()); grew || amount != 0 {
		t.Fatal("should not grow when too small")
	}
	if grew, amount := buf.GrowTo(true, time.Now()); !grew || amount != 2 {
		t.Fatal("should have grown by 2")
	}

	if err := buf.Append(bytes.NewReader(make([]byte, 50)), 50); err != nil {
		t.Fatal(err)
	}
	assert(51, 49)
	if grew, amount := buf.GrowTo(false, time.Now()); grew || amount != 0 {
		t.Fatal("should not grow when data hasn't been read")
	}
	read, err := io.CopyN(ioutil.Discard, &buf, 50)
	if err != nil {
		t.Fatal(err)
	}
	if read != 50 {
		t.Fatal("expected to read 50 bytes")
	}

	assert(1, 49)
	if grew, amount := buf.GrowTo(false, time.Now()); !grew || amount != 50 {
		t.Fatal("should have grown when below half, even with reserved space")
	}
	assert(1, 99)
}

func TestSegmentedBuffer_WindowAutoSizing(t *testing.T) {
	receiveAndConsume := func(buf *segmentedBuffer, size uint32) {
		if err := buf.Append(bytes.NewReader(make([]byte, size)), size); err != nil {
			t.Fatal(err)
		}
		if _, err := buf.Read(make([]byte, size)); err != nil {
			t.Fatal(err)
		}
	}
	const rtt = 10 * time.Millisecond
	const initialWindow uint32 = 10
	t.Run("finding the window size", func(t *testing.T) {
		buf := newSegmentedBuffer(initialWindow, 1000*initialWindow, func() time.Duration { return rtt })
		start := time.Now()
		// Consume a maximum of 1234 bytes per RTT.
		// We expect the window to be scaled such that we send one update every 2 RTTs.
		now := start
		delta := initialWindow
		for i := 0; i < 100; i++ {
			now = now.Add(rtt)
			size := delta
			if size > 1234 {
				size = 1234
			}
			receiveAndConsume(&buf, size)
			grow, d := buf.GrowTo(false, now)
			if grow {
				delta = d
			}
		}
		if !(buf.windowSize > 2*1234 && buf.windowSize < 3*1234) {
			t.Fatalf("unexpected window size: %d", buf.windowSize)
		}
	})
	t.Run("capping the window size", func(t *testing.T) {
		const maxWindow = 78 * initialWindow
		buf := newSegmentedBuffer(initialWindow, maxWindow, func() time.Duration { return rtt })
		start := time.Now()
		// Consume a maximum of 1234 bytes per RTT.
		// We expect the window to be scaled such that we send one update every 2 RTTs.
		now := start
		delta := initialWindow
		for i := 0; i < 100; i++ {
			now = now.Add(rtt)
			receiveAndConsume(&buf, delta)
			grow, d := buf.GrowTo(false, now)
			if grow {
				delta = d
			}
		}
		if buf.windowSize != maxWindow {
			t.Fatalf("expected the window size to be at max (%d), got %d", maxWindow, buf.windowSize)
		}
	})
}
