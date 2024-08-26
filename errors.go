package yamux

import "fmt"

type Error struct {
	msg                string
	timeout, temporary bool
}

func (ye *Error) Error() string {
	return ye.msg
}

func (ye *Error) Timeout() bool {
	return ye.timeout
}

func (ye *Error) Temporary() bool {
	return ye.temporary
}

type GoAwayError struct {
	Remote    bool
	ErrorCode uint32
}

func (e *GoAwayError) Error() string {
	if e.Remote {
		return fmt.Sprintf("remote sent go away, code: %d", e.ErrorCode)
	}
	return fmt.Sprintf("sent go away, code: %d", e.ErrorCode)
}

func (e *GoAwayError) Timeout() bool {
	return false
}

func (e *GoAwayError) Temporary() bool {
	return false
}

func (e *GoAwayError) Is(target error) bool {
	// to maintain compatibility with errors returned by previous versions
	if e.Remote && target == ErrRemoteGoAway {
		return true
	} else if !e.Remote && target == ErrSessionShutdown {
		return true
	}

	if err, ok := target.(*GoAwayError); ok {
		return *e == *err
	}
	return false
}

var (
	// ErrInvalidVersion means we received a frame with an
	// invalid version
	ErrInvalidVersion = &Error{msg: "invalid protocol version"}

	// ErrInvalidMsgType means we received a frame with an
	// invalid message type
	ErrInvalidMsgType = &Error{msg: "invalid msg type"}

	// ErrSessionShutdown is used if there is a shutdown during
	// an operation
	ErrSessionShutdown = &Error{msg: "session shutdown"}

	// ErrStreamsExhausted is returned if we have no more
	// stream ids to issue
	ErrStreamsExhausted = &Error{msg: "streams exhausted"}

	// ErrDuplicateStream is used if a duplicate stream is
	// opened inbound
	ErrDuplicateStream = &Error{msg: "duplicate stream initiated"}

	// ErrReceiveWindowExceeded indicates the window was exceeded
	ErrRecvWindowExceeded = &Error{msg: "recv window exceeded"}

	// ErrTimeout is used when we reach an IO deadline
	ErrTimeout = &Error{msg: "i/o deadline reached", timeout: true, temporary: true}

	// ErrStreamClosed is returned when using a closed stream
	ErrStreamClosed = &Error{msg: "stream closed"}

	// ErrUnexpectedFlag is set when we get an unexpected flag
	ErrUnexpectedFlag = &Error{msg: "unexpected flag"}

	// ErrRemoteGoAway is used when we get a go away from the other side
	ErrRemoteGoAway = &Error{msg: "remote end is not accepting connections"}

	// ErrStreamReset is sent if a stream is reset. This can happen
	// if the backlog is exceeded, or if there was a remote GoAway.
	ErrStreamReset = &Error{msg: "stream reset"}

	// ErrConnectionWriteTimeout indicates that we hit the "safety valve"
	// timeout writing to the underlying stream connection.
	ErrConnectionWriteTimeout = &Error{msg: "connection write timeout", timeout: true}

	// ErrKeepAliveTimeout is sent if a missed keepalive caused the stream close
	ErrKeepAliveTimeout = &Error{msg: "keepalive timeout", timeout: true}
)
