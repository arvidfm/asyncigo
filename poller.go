package asyngio

import (
	"context"
	"io"
	"time"
)

// Poller represents a type that can wait for multiple I/O events simultaneously.
type Poller interface {
	// Close closes this Poller.
	Close() error
	// Wait waits for one or more I/O events. If an I/O event occurs, Wait
	// should call the [AsyncReadWriteCloser.NotifyReady] method of the associated
	// file handle.
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
	// WaitForReady suspends the calling coroutine until [AsyncReadWriteCloser.NotifyReady] is called.
	WaitForReady(ctx context.Context) error
	// NotifyReady informs the file handle that data is available, or that data has been written.
	NotifyReady()
}
