//go:build linux && epoll

package asyncio_go

import (
	"context"
	"encoding/binary"
	"errors"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"os"
	"runtime"
	"time"
)

type EpollPoller struct {
	epfd     int
	waker    io.ReadWriteCloser
	wakerBuf []byte

	subscribed map[int32]AsyncFder
	events     []unix.EpollEvent
}

func NewPoller() (Poller, error) {
	epfd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}

	poller := &EpollPoller{
		epfd:       epfd,
		wakerBuf:   make([]byte, 8),
		subscribed: make(map[int32]AsyncFder),
		events:     make([]unix.EpollEvent, 10),
	}

	// eventfd for waking up the poller from another thread
	wakerFd, err := unix.Eventfd(0, unix.EFD_NONBLOCK)
	if err != nil {
		return nil, err
	}
	poller.waker, err = poller.Open(uintptr(wakerFd))
	if err != nil {
		return nil, err
	}

	return poller, nil
}

func (e *EpollPoller) Wait(timeout time.Duration) error {
	n, err := unix.EpollWait(e.epfd, e.events, max(0, int(timeout.Milliseconds())))
	if err != nil {
		if errors.Is(err, unix.EINTR) {
			err = nil
		}
		return err
	}

	for i := 0; i < n; i++ {
		fd := e.events[i].Fd
		if file := e.subscribed[fd]; file != nil {
			file.NotifyReady()
		}
	}

	return nil
}

func (e *EpollPoller) WakeupThreadsafe() error {
	buf := make([]byte, 8)
	binary.NativeEndian.PutUint64(buf, 1)
	_, err := e.waker.Write(buf)
	return err
}

func (e *EpollPoller) Subscribe(target AsyncFder) error {
	fd := int(target.Fd())
	if err := unix.SetNonblock(fd, true); err != nil {
		return err
	}

	event := unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLOUT | unix.EPOLLPRI | unix.EPOLLET, Fd: int32(fd)}
	if err := unix.EpollCtl(e.epfd, unix.EPOLL_CTL_ADD, fd, &event); err != nil {
		return err
	}
	e.subscribed[int32(fd)] = target

	return nil
}

func (e *EpollPoller) Unsubscribe(target AsyncFder) error {
	fd := int(target.Fd())
	delete(e.subscribed, int32(fd))
	return unix.EpollCtl(e.epfd, unix.EPOLL_CTL_DEL, fd, nil)
}

func (e *EpollPoller) Open(fd uintptr) (file AsyncFder, err error) {
	f := &EpollAsyncFile{
		f:      os.NewFile(fd, ""),
		poller: e,
	}
	if err := e.Subscribe(f); err != nil {
		return nil, err
	}
	return f, nil
}

func (e *EpollPoller) Dial(ctx context.Context, network, address string) (conn AsyncFder, err error) {
	if network != "tcp" {
		return nil, errors.New("unsupported connection type")
	}

	// would have been nice to be able to rely on go's own intelligent address resolution,
	// and then just retrieve the underlying fd and perform non-blocking operations with it,
	// but there's no way to keep the connecting code from blocking,
	// so just do a simple, naive getaddrinfo/socket/connect

	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	portNum, err := net.DefaultResolver.LookupPort(ctx, network, port)
	if err != nil {
		return nil, err
	}

	// this is still blocking though :(
	// consider getaddrinfo_a???
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}

	// try to connect in parallel and return the first successful connection
	futs := make([]Coroutine2[*EpollAsyncFile], len(addrs))
	for i, addr := range addrs {
		futs[i] = func(ctx context.Context) (*EpollAsyncFile, error) {
			return e.dialSingle(ctx, addr, portNum)
		}
	}

	return GetFirstResult(ctx, futs...)
}

func (e *EpollPoller) dialSingle(ctx context.Context, addr net.IPAddr, port int) (*EpollAsyncFile, error) {
	domain, sockAddr, err := e.toSockAddr(addr, port)
	if err != nil {
		return nil, err
	}

	fd, err := unix.Socket(domain, unix.SOCK_STREAM|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}

	f := NewEpollAsyncFile(e, NewSocket(fd))
	if err := e.Subscribe(f); err != nil {
		_ = f.Close()
		return nil, err
	}

	for {
		err := unix.Connect(fd, sockAddr)
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINPROGRESS) || errors.Is(err, unix.EALREADY) {
			if err := f.WaitForReady(ctx); err != nil {
				_ = f.Close()
				return nil, err
			}
			continue
		}

		if err != nil {
			_ = f.Close()
			return nil, err
		} else {
			return f, nil
		}
	}
}

func (e *EpollPoller) toSockAddr(addr net.IPAddr, port int) (domain int, sockAddr unix.Sockaddr, err error) {
	if ipv4 := addr.IP.To4(); len(ipv4) == net.IPv4len {
		return unix.AF_INET, &unix.SockaddrInet4{Port: port, Addr: [net.IPv4len]byte(ipv4)}, nil
	} else if ipv6 := addr.IP.To16(); len(ipv6) == net.IPv6len {
		// handling the zone seems really complicated so no thanks
		return unix.AF_INET6, &unix.SockaddrInet6{Port: port, Addr: [net.IPv6len]byte(ipv6)}, nil
	} else {
		return domain, nil, errors.New("could not parse IP address")
	}
}

type EpollAsyncFile struct {
	poller   *EpollPoller
	f        Fder
	readyFut *Future[any]
}

func NewEpollAsyncFile(poller *EpollPoller, f Fder) *EpollAsyncFile {
	eaf := &EpollAsyncFile{
		f:      f,
		poller: poller,
	}
	runtime.SetFinalizer(eaf, func(e *EpollAsyncFile) { _ = e.Close() })
	return eaf
}

func (eaf *EpollAsyncFile) NotifyReady() {
	if eaf.readyFut != nil {
		eaf.readyFut.SetResult(nil, nil)
	}
}

func (eaf *EpollAsyncFile) WaitForReady(ctx context.Context) error {
	eaf.readyFut = NewFuture[any]()
	_, err := eaf.readyFut.Await(ctx)
	return err
}

func (eaf *EpollAsyncFile) Read(p []byte) (n int, err error) {
	return eaf.f.Read(p)
}

func (eaf *EpollAsyncFile) Write(p []byte) (n int, err error) {
	return eaf.f.Write(p)
}

func (eaf *EpollAsyncFile) Close() error {
	_ = eaf.poller.Unsubscribe(eaf)
	return eaf.f.Close()
}

func (eaf *EpollAsyncFile) Fd() uintptr {
	return eaf.f.Fd()
}

type EpollSocket struct {
	fd int
}

func (s *EpollSocket) Read(p []byte) (n int, err error) {
	n, err = unix.Read(s.fd, p)
	if n == 0 && err == nil {
		err = io.EOF
	}
	return n, err
}

func (s *EpollSocket) Write(p []byte) (n int, err error) {
	return unix.Write(s.fd, p)
}

func (s *EpollSocket) Close() error {
	return unix.Close(s.fd)
}

func (s *EpollSocket) Fd() uintptr {
	return uintptr(s.fd)
}

func NewSocket(fd int) *EpollSocket {
	s := &EpollSocket{fd: fd}
	runtime.SetFinalizer(s, func(s *EpollSocket) { _ = s.Close() })
	return s
}
