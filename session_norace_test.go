//+build !race

package yamux

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"sync"
	"testing"
	"time"
)

func TestSession_PingOfDeath(t *testing.T) {
	conf := testConfNoKeepAlive()
	// This test is slow and can easily time out on writes on CI.
	//
	// In the future, we might want to prioritize ping-replies over even
	// other control messages, but that seems like overkill for now.
	conf.ConnectionWriteTimeout = 1 * time.Second
	client, server := testClientServerConfig(conf)
	defer client.Close()
	defer server.Close()

	count := 10000

	var wg sync.WaitGroup
	begin := make(chan struct{})
	for i := 0; i < count; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-begin
			if _, err := server.Ping(); err != nil {
				t.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			<-begin
			if _, err := client.Ping(); err != nil {
				t.Error(err)
			}
		}()
	}
	close(begin)
	wg.Wait()
}

func TestSendData_VeryLarge(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	var n int64 = 1 * 1024 * 1024 * 1024
	var workers int = 16

	wg := &sync.WaitGroup{}
	wg.Add(workers * 2)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			stream, err := server.AcceptStream()
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			defer stream.Close()

			buf := make([]byte, 4)
			_, err = io.ReadFull(stream, buf)
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if !bytes.Equal(buf, []byte{0, 1, 2, 3}) {
				t.Errorf("bad header")
				return
			}

			recv, err := io.Copy(ioutil.Discard, stream)
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if recv != n {
				t.Errorf("bad: %v", recv)
				return
			}
		}()
	}
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			stream, err := client.Open(context.Background())
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			defer stream.Close()

			_, err = stream.Write([]byte{0, 1, 2, 3})
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}

			unlimited := &UnlimitedReader{}
			sent, err := io.Copy(stream, io.LimitReader(unlimited, n))
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if sent != n {
				t.Errorf("bad: %v", sent)
				return
			}
		}()
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(20 * time.Second):
		server.Close()
		client.Close()
		wg.Wait()
		t.Fatal("timeout")
	}
}

func TestLargeWindow(t *testing.T) {
	conf := DefaultConfig()
	conf.MaxStreamWindowSize *= 2

	client, server := testClientServerConfig(conf)
	defer client.Close()
	defer server.Close()

	stream, err := client.Open(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer stream.Close()

	stream2, err := server.Accept()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer stream2.Close()

	err = stream.SetWriteDeadline(time.Now().Add(10 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, conf.MaxStreamWindowSize)
	n, err := stream.Write(buf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("short write: %d", n)
	}
}
