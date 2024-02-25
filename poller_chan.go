//go:build !linux || !epoll

package asyncigo

import (
	"context"
	"io"
	"syscall"
	"time"
)

// ChannelPoller is a basic channel-based poller which does not support network I/O.
type ChannelPoller struct {
	wakeupCh    chan struct{}
	subscribers map[channelNotifier]struct{}
}

// NewPoller constructs a new [ChannelPoller].
func NewPoller() (Poller, error) {
	return &ChannelPoller{
		wakeupCh:    make(chan struct{}, 100),
		subscribers: make(map[channelNotifier]struct{}),
	}, nil
}

// Wait implements [Poller].
func (c *ChannelPoller) Wait(timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// we don't support network i/o so we assume writes can only happen from coroutines,
	// meaning we can just check whether the channels are ready upfront
	// instead of polling while sleeping
	var wasReady bool
	for subscriber := range c.subscribers {
		wasReady = wasReady || subscriber.notifyReadyMaybe()
	}

	if wasReady {
		return nil
	}

	// none of our channels were ready, so sleep until the next callback
	select {
	case <-timer.C:
	case <-c.wakeupCh:
	}
	return nil
}

// Subscribe registers a channel wrapper to be checked for readiness each tick.
func (c *ChannelPoller) Subscribe(f channelNotifier) {
	c.subscribers[f] = struct{}{}
}

// Unsubscribe stops a channel wrapper from being checked for readiness each tick.
func (c *ChannelPoller) Unsubscribe(f channelNotifier) {
	delete(c.subscribers, f)
}

// WakeupThreadsafe implements [Poller].
func (c *ChannelPoller) WakeupThreadsafe() error {
	c.wakeupCh <- struct{}{}
	return nil
}

// Close implements [Poller].
func (c *ChannelPoller) Close() error {
	close(c.wakeupCh)
	return nil
}

// Pipe implements [Poller].
func (c *ChannelPoller) Pipe() (r, w AsyncReadWriteCloser, err error) {
	ch := make(chan []byte, 100)
	rc := NewChannelReader(c, ch)
	wc := NewChannelWriter(c, ch)
	c.Subscribe(rc)
	c.Subscribe(wc)
	return rc, wc, nil
}

// Dial implements [Poller]·
func (c *ChannelPoller) Dial(_ context.Context, _, _ string) (AsyncReadWriteCloser, error) {
	return nil, ErrNotImplemented
}

type channelNotifier interface {
	// notifyReadyMaybe notifies any waiting coroutines
	// if the channel is ready to be read from/written to.
	// Returns false if the channel was not ready.
	notifyReadyMaybe() bool
}

// ChannelFile is a base type embedded by [ChannelReader] and [ChannelWriter].
type ChannelFile struct {
	readyFut *Future[any]
}

// WaitForReady implements [AsyncReadWriteCloser].
func (c *ChannelFile) WaitForReady(ctx context.Context) error {
	if c.readyFut == nil {
		c.readyFut = NewFuture[any]()
	}
	_, err := c.readyFut.Await(ctx)
	return err
}

func (c *ChannelFile) notifyReady() bool {
	if c.readyFut != nil {
		readyFut := c.readyFut
		c.readyFut = nil
		readyFut.SetResult(nil, nil)
		return true
	}
	return false
}

// ChannelReader is an asynchronous file-like object wrapping a receiving channel.
type ChannelReader struct {
	ChannelFile
	poller *ChannelPoller
	ch     <-chan []byte
	buffer []byte
}

// NewChannelReader returns a readable file-like object wrapping the given channel.
func NewChannelReader(poller *ChannelPoller, ch <-chan []byte) *ChannelReader {
	return &ChannelReader{poller: poller, ch: ch}
}

// notifyReadyMaybe implements channelNotifier.
func (c *ChannelReader) notifyReadyMaybe() bool {
	if len(c.ch) > 0 {
		// notify if we have available data
		return c.notifyReady()
	}
	select {
	case _, ok := <-c.ch:
		if !ok {
			// also notify about channel close
			return c.notifyReady()
		}
	default:
	}
	return false
}

// Read implements [io.Reader].
func (c *ChannelReader) Read(p []byte) (n int, err error) {
	if c.ch == nil {
		return 0, syscall.EBADF
	}

loop:
	for len(c.buffer) < len(p) {
		select {
		case chunk, ok := <-c.ch:
			if !ok {
				if len(c.buffer) == 0 {
					return 0, io.EOF
				}
				break loop
			}
			c.buffer = append(c.buffer, chunk...)
		default:
			break loop
		}
	}

	if len(c.buffer) == 0 {
		return 0, syscall.EAGAIN
	}

	n = copy(p, c.buffer)
	c.buffer = c.buffer[n:]
	return n, nil
}

// Write implements [io.Writer].
func (c *ChannelReader) Write(p []byte) (n int, err error) {
	return 0, syscall.EBADF
}

// Close implements [io.Closer]·
func (c *ChannelReader) Close() error {
	if c.ch == nil {
		return syscall.EBADF
	}
	c.poller.Unsubscribe(c)
	c.ch = nil
	return nil
}

// ChannelWriter is an asynchronous file-like object wrapping a sending channel.
type ChannelWriter struct {
	ChannelFile
	poller *ChannelPoller
	ch     chan<- []byte
}

// NewChannelWriter returns a writeable file-like object wrapping the given channel.
func NewChannelWriter(poller *ChannelPoller, ch chan<- []byte) *ChannelWriter {
	return &ChannelWriter{poller: poller, ch: ch}
}

// notifyReadyMaybe implements channelNotifier.
func (c *ChannelWriter) notifyReadyMaybe() bool {
	if len(c.ch) < cap(c.ch) {
		return c.notifyReady()
	}
	return false
}

// Read implements [io.Reader].
func (c *ChannelWriter) Read(p []byte) (n int, err error) {
	return 0, syscall.EBADF
}

// Write implements [io.Writer].
func (c *ChannelWriter) Write(p []byte) (n int, err error) {
	if c.ch == nil {
		return 0, syscall.EBADF
	}

	select {
	case c.ch <- p:
		return len(p), nil
	default:
		return 0, syscall.EAGAIN
	}
}

// Close implements [io.Closer].
func (c *ChannelWriter) Close() error {
	if c.ch == nil {
		return syscall.EBADF
	}
	c.poller.Unsubscribe(c)
	close(c.ch)
	c.ch = nil
	return nil
}
