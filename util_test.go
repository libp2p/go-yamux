package yamux

import (
	"bytes"
	"io"
	"testing"
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
	buf := newSegmentedBuffer(100)
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
	if grew, amount := buf.GrowTo(100, false); grew || amount != 0 {
		t.Fatal("should not grow when too small")
	}
	if grew, amount := buf.GrowTo(100, true); !grew || amount != 2 {
		t.Fatal("should have grown by 2")
	}

	if err := buf.Append(bytes.NewReader(make([]byte, 50)), 50); err != nil {
		t.Fatal(err)
	}
	assert(51, 49)
	if grew, amount := buf.GrowTo(100, false); grew || amount != 0 {
		t.Fatal("should not grow when data hasn't been read")
	}
	read, err := io.CopyN(io.Discard, &buf, 50)
	if err != nil {
		t.Fatal(err)
	}
	if read != 50 {
		t.Fatal("expected to read 50 bytes")
	}

	assert(1, 49)
	if grew, amount := buf.GrowTo(100, false); !grew || amount != 50 {
		t.Fatal("should have grown when below half, even with reserved space")
	}
	assert(1, 99)
}
