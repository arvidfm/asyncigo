package asyncio_go

import (
	"context"
	"io"
	"time"
)

type Poller interface {
	Wait(timeout time.Duration) error
	WakeupThreadsafe() error
	Open(fd uintptr) (AsyncFder, error)
	Dial(ctx context.Context, network, address string) (AsyncFder, error)
	Subscribe(target AsyncFder) error
	Unsubscribe(target AsyncFder) error
}

type Fder interface {
	io.ReadWriteCloser
	Fd() uintptr
}

type AsyncFder interface {
	Fder
	WaitForReady(ctx context.Context) error
	NotifyReady()
}
