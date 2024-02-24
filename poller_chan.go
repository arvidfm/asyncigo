//go:build !linux || !epoll

package asyngio

import (
	"time"
)

type ChannelPoller struct {
	wakeupCh chan struct{}
}

func NewPoller() Poller {
	return &ChannelPoller{
		wakeupCh: make(chan struct{}, 100),
	}
}

func (c *ChannelPoller) Wait(timeout time.Duration, onTimeout func()) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		onTimeout()
	case <-c.wakeupCh:
	}

	return nil
}

func (c *ChannelPoller) WakeupThreadsafe() {
	c.wakeupCh <- struct{}{}
}
