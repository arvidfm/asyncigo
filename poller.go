package asyncigo

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
	// ErrNotImplemented is returned by a Poller implementation for functions it does not support.
	ErrNotImplemented = errors.New("this function is not supported by this implementation")
)

// Poller represents a type that can wait for multiple I/O events simultaneously.
type Poller interface {
	// Close closes this Poller.
	Close() error
	// Wait waits for one or more I/O events. If an I/O event occurs, Wait
	// should wake up any coroutines waiting on [AsyncReadWriteCloser.WaitForReady]
	// for the corresponding file handle.
	Wait(timeout time.Duration) error
	// WakeupThreadsafe instructs the Poller to stop waiting and return control to the event loop.
	WakeupThreadsafe() error
	// Pipe constructs a pair of asynchronous file handles where writing to w
	// causes the same data to be read from r.
	Pipe() (r, w AsyncReadWriteCloser, err error)
	// Dial opens a non-blocking network connection.
	Dial(ctx context.Context, network, address string) (AsyncReadWriteCloser, error)
}

// AsyncReadWriteCloser represents a non-blocking file handle.
//
// If an I/O operation is attempted when the underlying stream is not ready,
// e.g. because data is not yet available, a [syscall.EAGAIN] error will be returned.
type AsyncReadWriteCloser interface {
	io.ReadWriteCloser
	// WaitForReady suspends the calling coroutine until an I/O event occurs for this file handle.
	WaitForReady(ctx context.Context) error
}
