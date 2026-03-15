package yamux

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// delayPipe copies from src to dst, adding a one-way delay to each chunk
// to simulate network latency.
func delayPipe(dst, src net.Conn, delay time.Duration) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			time.Sleep(delay)
			dst.Write(buf[:n])
		}
		if err != nil {
			dst.Close()
			return
		}
	}
}

// makeLatencyPair creates a pair of net.Conn with simulated one-way latency
// in each direction (total RTT = 2 * oneWayDelay).
func makeLatencyPair(t *testing.T, oneWayDelay time.Duration) (net.Conn, net.Conn) {
	t.Helper()
	clientEnd, proxyA := net.Pipe()
	proxyB, serverEnd := net.Pipe()
	go delayPipe(proxyB, proxyA, oneWayDelay)
	go delayPipe(proxyA, proxyB, oneWayDelay)
	return clientEnd, serverEnd
}

// TestAutotuningReachesMaxWindow verifies that the receive window grows from
// InitialStreamWindowSize to MaxStreamWindowSize under sustained throughput
// with real network latency.
//
// Without the scaleFactor fix, the window self-blocks at ~2x the initial size
// because the fixed 4*RTT threshold doesn't account for the increased time
// needed to fill a larger window. See https://github.com/libp2p/go-yamux/issues/136
func TestAutotuningReachesMaxWindow(t *testing.T) {
	const (
		initialWindow = 256 * 1024       // 256 KB
		maxWindow     = 4 * 1024 * 1024  // 4 MB
		oneWayDelay   = 10 * time.Millisecond // 20ms RTT
		dataToSend    = 8 * 1024 * 1024  // 8 MB — enough for 4 doublings
	)

	conf := DefaultConfig()
	conf.InitialStreamWindowSize = initialWindow
	conf.MaxStreamWindowSize = maxWindow
	conf.EnableKeepAlive = false
	conf.MeasureRTTInterval = 200 * time.Millisecond

	clientConn, serverConn := makeLatencyPair(t, oneWayDelay)

	clientSession, err := Client(clientConn, conf, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clientSession.Close()

	serverSession, err := Server(serverConn, conf, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer serverSession.Close()

	// Wait for at least one RTT measurement via MeasureRTTInterval.
	time.Sleep(500 * time.Millisecond)

	// Server goroutine: accept stream and write data.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		stream, err := serverSession.AcceptStream()
		if err != nil {
			return
		}
		defer stream.Close()
		chunk := make([]byte, 32*1024)
		remaining := dataToSend
		for remaining > 0 {
			n := len(chunk)
			if n > remaining {
				n = remaining
			}
			written, err := stream.Write(chunk[:n])
			remaining -= written
			if err != nil {
				return
			}
		}
	}()

	// Client: open stream and read all data.
	stream, err := clientSession.OpenStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 64*1024)
	totalRead := 0
	for totalRead < dataToSend {
		n, err := stream.Read(buf)
		totalRead += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	// Check window BEFORE closing the stream.
	finalWindow := stream.recvWindow
	stream.Close()
	wg.Wait()

	if finalWindow < maxWindow {
		t.Errorf("autotuning did not reach max window: got %d KB, want %d KB",
			finalWindow/1024, maxWindow/1024)
	}
	t.Logf("autotuning OK: window grew from %d KB to %d KB",
		initialWindow/1024, finalWindow/1024)
}
