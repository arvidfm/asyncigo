package asyncio_go

import (
	"container/heap"
	"context"
	"errors"
	"io"
	"iter"
	"log/slog"
	"os"
	"slices"
	"syscall"
	"time"
)

type Futurer interface {
	HasResult() bool
	Err() error
	AddDoneCallback(callback func(error)) Futurer
	Cancel(err error)
}

type Awaitable[T any] interface {
	Futurer
	Await(ctx context.Context) (T, error)
	MustAwait(ctx context.Context) T
	AddResultCallback(callback func(result T, err error)) Awaitable[T]
	WriteResultTo(dst *T) Awaitable[T]
	Future() *Future[T]
}

type Future[ResType any] struct {
	ctx            context.Context
	done           bool
	err            error
	result         ResType
	callbacks      []func(ResType, error)
	awaitCallbacks []func(*Future[ResType], *EventLoop)
	name           string
}

func NewFuture[ResType any]() *Future[ResType] {
	return &Future[ResType]{}
}

func (f *Future[ResType]) WithName(name string) *Future[ResType] {
	f.name = name
	return f
}

func (f *Future[ResType]) HasResult() bool {
	return f.done
}

func (f *Future[ResType]) Err() error {
	return f.err
}

func (f *Future[ResType]) Result() (ResType, error) {
	if f.done {
		return f.result, f.err
	}

	var zero ResType
	err := f.ctx.Err()
	if err == nil {
		err = errors.New("future is still pending")
	}
	return zero, err
}

func (f *Future[ResType]) Future() *Future[ResType] {
	return f
}

func (f *Future[ResType]) AddDoneCallback(callback func(error)) Futurer {
	f.AddResultCallback(func(_ ResType, err error) {
		callback(err)
	})
	return f
}

func (f *Future[ResType]) AddResultCallback(callback func(ResType, error)) Awaitable[ResType] {
	if f.HasResult() {
		callback(f.result, f.err)
	} else {
		f.callbacks = append(f.callbacks, callback)
	}
	return f
}

func (f *Future[ResType]) WriteResultTo(dest *ResType) Awaitable[ResType] {
	return f.AddResultCallback(func(result ResType, err error) {
		if err == nil {
			*dest = result
		}
	})
}

func (f *Future[ResType]) OnAwait(callback func(fut *Future[ResType], loop *EventLoop)) *Future[ResType] {
	f.awaitCallbacks = append(f.awaitCallbacks, callback)
	return f
}

func (f *Future[ResType]) Await(ctx context.Context) (ResType, error) {
	loop := RunningLoop(ctx)
	for _, callback := range f.awaitCallbacks {
		callback(f, loop)
	}
	f.awaitCallbacks = nil

	yield := ctx.Value(yielder{}).(Yielder)
	if err := yield(ctx, f); err != nil {
		var zero ResType
		return zero, err
	}
	return f.Result()
}

func (f *Future[ResType]) MustAwait(ctx context.Context) ResType {
	res, err := f.Await(ctx)
	if err != nil {
		panic(err)
	}
	return res
}

func (f *Future[ResType]) Cancel(err error) {
	if err == nil {
		err = context.Canceled
	}
	var zero ResType
	f.SetResult(zero, err)
}

func (f *Future[ResType]) Shield() *Future[ResType] {
	if f.HasResult() {
		return f
	}

	fut := NewFuture[ResType]()
	f.AddResultCallback(func(result ResType, err error) {
		fut.SetResult(result, err)
	})
	fut.AddResultCallback(func(result ResType, err error) {
		if !errors.Is(err, context.Canceled) {
			f.SetResult(result, err)
		}
	})
	return fut
}

func (f *Future[ResType]) SetResult(result ResType, err error) {
	if f.HasResult() {
		return
	}

	f.result, f.err = result, err
	f.done = true

	for _, callback := range f.callbacks {
		callback(result, err)
	}
}

type Task[RetType any] struct {
	next       func() (Futurer, bool)
	stop       func()
	cancel     context.CancelCauseFunc
	pendingFut Futurer
	resultFut  *Future[RetType]
}

func SpawnTask[RetType any](ctx context.Context, coro Coroutine2[RetType]) *Task[RetType] {
	ctx, cancel := context.WithCancelCause(ctx)
	task := &Task[RetType]{
		resultFut: NewFuture[RetType](),
		cancel:    cancel,
	}
	next, stop := iter.Pull(func(yield func(Futurer) bool) {
		ctx := context.WithValue(ctx, yielder{}, Yielder(func(childCtx context.Context, fut Futurer) error {
			if err := context.Cause(ctx); err != nil {
				task.resultFut.Cancel(err)
				fut.Cancel(err)
				return task.Err()
			}
			if err := childCtx.Err(); err != nil {
				fut.Cancel(err)
				return err
			}
			if !yield(fut) {
				task.resultFut.Cancel(nil)
				return task.Err()
			}
			if err := context.Cause(ctx); err != nil {
				task.resultFut.Cancel(err)
				return task.Err()
			}
			return nil
		}))
		task.resultFut.SetResult(coro(ctx))
	})
	task.resultFut.AddDoneCallback(func(err error) {
		if task.pendingFut != nil {
			task.pendingFut.Cancel(nil)
		}
		task.cancel(err)
	})
	task.next = next
	task.stop = stop

	// don't start the coroutine if the context has already been cancelled
	if err := context.Cause(ctx); err != nil {
		task.resultFut.Cancel(err)
	} else {
		task.Step()
	}
	return task
}

func (t *Task[_]) Step() (ok bool) {
	if t.pendingFut, ok = t.next(); ok {
		t.pendingFut.AddDoneCallback(func(err error) {
			t.Step()
		})
		return true
	} else {
		t.pendingFut = nil
		t.stop()
		return false
	}
}

func (t *Task[_]) Stop() {
	t.stop()
}

func (t *Task[_]) HasResult() bool {
	return t.resultFut.HasResult()
}

func (t *Task[_]) Err() error {
	return t.resultFut.Err()
}

func (t *Task[RetType]) Future() *Future[RetType] {
	return t.resultFut
}

func (t *Task[RetType]) Await(ctx context.Context) (RetType, error) {
	return t.resultFut.Await(ctx)
}

func (t *Task[RetType]) MustAwait(ctx context.Context) RetType {
	return t.resultFut.MustAwait(ctx)
}

func (t *Task[RetType]) WriteResultTo(dst *RetType) Awaitable[RetType] {
	t.resultFut.WriteResultTo(dst)
	return t
}

func (t *Task[_]) Cancel(err error) {
	t.resultFut.Cancel(err)
}

func (t *Task[RetType]) AddResultCallback(callback func(result RetType, err error)) Awaitable[RetType] {
	t.resultFut.AddResultCallback(callback)
	return t
}

func (t *Task[_]) AddDoneCallback(callback func(error)) Futurer {
	t.resultFut.AddDoneCallback(callback)
	return t
}

type Callback struct {
	callback func()
	doneCh   <-chan time.Time
	when     time.Time
}

type callbackQueue []Callback

func (r *callbackQueue) Len() int {
	return len(*r)
}

func (r *callbackQueue) Less(i, j int) bool {
	return (*r)[i].when.Before((*r)[j].when)
}

func (r *callbackQueue) Swap(i, j int) {
	(*r)[i], (*r)[j] = (*r)[j], (*r)[i]
}

func (r *callbackQueue) Push(x any) {
	*r = append(*r, x.(Callback))
}

func (r *callbackQueue) Pop() (v any) {
	n := len(*r)
	v, *r = (*r)[n-1], (*r)[:n-1]
	return v
}

func (r *callbackQueue) Peek() Callback {
	return (*r)[0]
}

func (r *callbackQueue) Add(c Callback) {
	heap.Push(r, c)
}

func (r *callbackQueue) RunFirst() {
	head := r.Peek()
	heap.Pop(r)
	head.callback()
}

func (r *callbackQueue) TimeUntilFirst() time.Duration {
	return time.Until(r.Peek().when)
}

func (r *callbackQueue) Empty() bool {
	return r.Len() == 0
}

type yielder struct{}
type runningLoop struct{}

type Yielder func(childCtx context.Context, fut Futurer) error

func RunningLoop(ctx context.Context) *EventLoop {
	return ctx.Value(runningLoop{}).(*EventLoop)
}

type EventLoop struct {
	pendingCallbacks    callbackQueue
	callbacksFromThread chan Callback

	poller Poller
}

func NewEventLoop() *EventLoop {
	return &EventLoop{
		callbacksFromThread: make(chan Callback, 100),
	}
}

func (e *EventLoop) Run(ctx context.Context, main Coroutine1) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	var err error
	if e.poller, err = NewPoller(); err != nil {
		return err
	}

	ctx = context.WithValue(ctx, runningLoop{}, e)
	mainTask := main.SpawnTask(ctx).Future().AddDoneCallback(func(err error) {
		if err != nil {
			cancel(err)
		}
	})

	for ctx.Err() == nil {
		e.addCallbacksFromThread(ctx)
		e.runReadyCallbacks(ctx)

		if ctx.Err() != nil || (mainTask.HasResult() && e.pendingCallbacks.Empty()) {
			break
		}

		timeout := time.Second * 30
		if !e.pendingCallbacks.Empty() {
			timeout = e.pendingCallbacks.TimeUntilFirst()
		}
		if err := e.poller.Wait(timeout); err != nil {
			return err
		}
	}

	return context.Cause(ctx)
}

func (e *EventLoop) addCallbacksFromThread(ctx context.Context) {
	for ctx.Err() == nil {
		select {
		case callback := <-e.callbacksFromThread:
			e.pendingCallbacks.Add(callback)
		default:
			return
		}
	}
}

func (e *EventLoop) runReadyCallbacks(ctx context.Context) {
	for ctx.Err() == nil && !e.pendingCallbacks.Empty() && e.pendingCallbacks.TimeUntilFirst() <= 0 {
		e.pendingCallbacks.RunFirst()
	}
}

func (e *EventLoop) ScheduleCallback(delay time.Duration, callback func()) {
	heap.Push(&e.pendingCallbacks, Callback{
		callback: callback,
		when:     time.Now().Add(delay),
		doneCh:   time.After(delay),
	})
}

func (e *EventLoop) RunCallback(callback func()) {
	e.ScheduleCallback(0, callback)
}

func (e *EventLoop) RunCallbackThreadsafe(callback func()) {
	e.callbacksFromThread <- Callback{
		callback: callback,
		when:     time.Now(),
		doneCh:   time.After(0),
	}
	if e.poller != nil {
		if err := e.poller.WakeupThreadsafe(); err != nil {
			slog.Warn("could not wake up event loop from thread", slog.Any("error", err))
		}
	}
}

func (e *EventLoop) NewAsyncStream(fd uintptr) (*AsyncStream, error) {
	f, err := e.poller.Open(fd)
	if err != nil {
		return nil, err
	}
	return NewAsyncStream(f), nil
}

func (e *EventLoop) Pipe() (r *AsyncStream, w *AsyncStream, err error) {
	rf, wf, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}

	if r, err = e.NewAsyncStream(rf.Fd()); err != nil {
		_ = rf.Close()
		_ = wf.Close()
		return nil, nil, err
	}
	if w, err = e.NewAsyncStream(wf.Fd()); err != nil {
		_ = rf.Close()
		_ = wf.Close()
		_ = r.Close()
		return nil, nil, err
	}
	return r, w, nil
}

func (e *EventLoop) Dial(ctx context.Context, network, address string) (*AsyncStream, error) {
	f, err := e.poller.Dial(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return NewAsyncStream(f), nil
}

type AsyncStream struct {
	file AsyncFder

	buffer   []byte
	readyFut *Future[any]

	writeLock Mutex
}

type SubscribeFunc func(onReady func()) (unsubscribe func() error, err error)

func NewAsyncStream(file AsyncFder) *AsyncStream {
	return &AsyncStream{
		file:     file,
		readyFut: NewFuture[any](),
	}
}

func (a *AsyncStream) Close() error {
	return a.file.Close()
}

func (a *AsyncStream) read(ctx context.Context, maxBytes int) (n int, err error) {
	if len(a.buffer) >= maxBytes {
		return maxBytes, nil
	}

	if cap(a.buffer) < maxBytes {
		a.buffer = slices.Grow(a.buffer, maxBytes)
	}

	for {
		readN, err := a.file.Read(a.buffer[len(a.buffer):maxBytes])
		if readN > 0 {
			a.buffer = a.buffer[:len(a.buffer)+readN]
		}

		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			if err = a.file.WaitForReady(ctx); err == nil {
				continue
			}
		}

		return len(a.buffer), err
	}
}

func (a *AsyncStream) Write(ctx context.Context, data []byte) Awaitable[int] {
	return SpawnTask(ctx, func(ctx context.Context) (int, error) {
		if err := a.writeLock.Lock(ctx); err != nil {
			return 0, err
		}
		defer a.writeLock.Unlock()

		var bytesWritten int
		for {
			n, err := a.file.Write(data)
			if n > 0 {
				bytesWritten += n
				data = data[n:]
			}

			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				err = a.file.WaitForReady(ctx)
			}
			if err != nil || len(data) == 0 {
				return bytesWritten, err
			}
		}
	})
}

func (a *AsyncStream) consumeInto(buf []byte) (n int) {
	n = copy(buf, a.buffer)
	copy(a.buffer, a.buffer[n:])
	a.buffer = a.buffer[:len(a.buffer)-n]
	return n
}

func (a *AsyncStream) consume(maxBytes int) []byte {
	buf := make([]byte, min(maxBytes, len(a.buffer)))
	n := a.consumeInto(buf)
	return buf[:n]
}

func (a *AsyncStream) consumeAll() []byte {
	buf := slices.Clone(a.buffer)
	a.buffer = a.buffer[:0]
	return buf
}

func (a *AsyncStream) Stream(ctx context.Context, bufSize int) AsyncIterable[[]byte] {
	return AsyncIter(func(yield func([]byte) error) error {
		for {
			n, err := a.read(ctx, bufSize)
			if n > 0 {
				if err := yield(a.consumeAll()); err != nil {
					return err
				}
			}
			if errors.Is(err, io.EOF) {
				return nil
			} else if err != nil {
				return err
			}
		}
	})
}

func (a *AsyncStream) Chunks(ctx context.Context, chunkSize int) AsyncIterable[[]byte] {
	return AsyncIter(func(yield func([]byte) error) error {
		for {
			var err error
			for len(a.buffer) < chunkSize && err == nil {
				_, err = a.read(ctx, chunkSize)
			}
			if len(a.buffer) > 0 {
				if err := yield(a.consume(chunkSize)); err != nil {
					return err
				}
			}
			if errors.Is(err, io.EOF) {
				return nil
			} else if err != nil {
				return err
			}
		}
	})
}

func (a *AsyncStream) yieldLines(yield func([]byte) error, data []byte) error {
	start := 0
	for i, b := range data {
		if b == '\n' || i == len(data)-1 {
			if err := yield(data[start : i+1]); err != nil {
				return err
			}
			start = i + 1
		}
	}
	return nil
}

func (a *AsyncStream) Lines(ctx context.Context) AsyncIterable[[]byte] {
	return AsyncIter(func(yield func([]byte) error) error {
		bufSize := 1024
		scanned := 0
		for {
			_, err := a.read(ctx, bufSize)
			if errors.Is(err, io.EOF) {
				return a.yieldLines(yield, a.consumeAll())
			} else if err != nil {
				return err
			}

			for i := len(a.buffer) - 1; i >= scanned; i-- {
				if a.buffer[i] == '\n' {
					if err := a.yieldLines(yield, a.consume(i+1)); err != nil {
						return err
					}
					break
				}
			}
			scanned = len(a.buffer)
			if len(a.buffer) >= bufSize {
				bufSize *= 2
			}
		}
	})
}

func (a *AsyncStream) ReadLine(ctx context.Context) ([]byte, error) {
	return a.ReadUntil(ctx, '\n')
}

func (a *AsyncStream) ReadUntil(ctx context.Context, character byte) ([]byte, error) {
	for i, b := range a.buffer {
		if b == character {
			return a.consume(i + 1), nil
		}
	}

	bufSize := 1024
	for {
		n, err := a.read(ctx, bufSize)
		for i := len(a.buffer) - n; i < len(a.buffer); i++ {
			if a.buffer[i] == character {
				return a.consume(i + 1), nil
			}
		}
		if errors.Is(err, io.EOF) && len(a.buffer) > 0 {
			return a.consumeAll(), nil
		} else if err != nil {
			return nil, err
		}

		if len(a.buffer) >= bufSize {
			bufSize *= 2
		}
	}
}

func (a *AsyncStream) ReadChunk(ctx context.Context, chunkSize int) ([]byte, error) {
	var err error
	for len(a.buffer) < chunkSize && err == nil {
		_, err = a.read(ctx, chunkSize)
	}
	if err == nil || errors.Is(err, io.EOF) && len(a.buffer) > 0 {
		return a.consume(chunkSize), nil
	}
	return nil, err
}

type Queue[T any] struct {
	data []T
	futs []*Future[T]
}

func (q *Queue[T]) Get() *Future[T] {
	fut := NewFuture[T]()
	if len(q.data) > 0 {
		item := q.data[0]
		q.data = q.data[1:]
		fut.SetResult(item, nil)
	}

	q.futs = append(q.futs, fut)
	return fut
}

func (q *Queue[T]) Push(item T) {
	q.data = append(q.data, item)
	for len(q.futs) > 0 && len(q.data) > 0 {
		// skip if cancelled
		if q.futs[0].HasResult() {
			q.futs = q.futs[1:]
			continue
		}

		fut, item := q.futs[0], q.data[0]
		q.futs, q.data = q.futs[1:], q.data[1:]
		fut.SetResult(item, nil)
	}
}

type Mutex struct {
	unlockFut *Future[any]
}

func (m *Mutex) Lock(ctx context.Context) error {
	for {
		if m.unlockFut == nil || m.unlockFut.HasResult() {
			m.unlockFut = NewFuture[any]()
			return nil
		}

		if _, err := m.unlockFut.Await(ctx); err != nil {
			return err
		}
	}
}

func (m *Mutex) Unlock() {
	if m.unlockFut != nil {
		m.unlockFut.SetResult(nil, nil)
	}
}

type Coroutine1 func(ctx context.Context) error

func (c Coroutine1) SpawnTask(ctx context.Context) *Task[any] {
	return SpawnTask[any](ctx, func(ctx context.Context) (any, error) {
		return nil, c(ctx)
	})
}

type Coroutine2[R any] func(ctx context.Context) (R, error)

func (c Coroutine2[R]) SpawnTask(ctx context.Context) *Task[R] {
	return SpawnTask(ctx, c)
}

type WaitMode int

const (
	WaitFirstResult WaitMode = iota
	WaitFirstError
	WaitAll
)

func Wait(mode WaitMode, futs ...Futurer) *Future[any] {
	var done int
	var futErr error
	waitFut := NewFuture[any]()
	waitFut.AddResultCallback(func(_ any, err error) {
		for _, fut := range futs {
			fut.Cancel(nil)
		}
	})

	for _, fut := range futs {
		fut.AddDoneCallback(func(err error) {
			done++
			if err != nil {
				futErr = err
				if mode != WaitAll {
					waitFut.SetResult(nil, err)
				}
			} else if done >= len(futs) || mode == WaitFirstResult {
				waitFut.SetResult(nil, futErr)
			}
		})
	}
	return waitFut
}

// GetFirstResult returns the result of the first successful Future.
// Once a Future succeeds, all other futures will be cancelled.
// If no Futures succeeded, the last error is returned.
func GetFirstResult[T any](futs ...Awaitable[T]) *Future[T] {
	var done int
	waitFut := NewFuture[T]()
	waitFut.AddResultCallback(func(_ T, err error) {
		for _, fut := range futs {
			fut.Cancel(nil)
		}
	})

	for _, fut := range futs {
		fut.AddResultCallback(func(result T, err error) {
			done++
			if err == nil {
				waitFut.SetResult(result, nil)
			} else if done >= len(futs) {
				waitFut.SetResult(result, err)
			}
		})
	}

	return waitFut
}

func Sleep(ctx context.Context, duration time.Duration) error {
	callTime := time.Now()
	_, err := NewFuture[any]().OnAwait(func(fut *Future[any], loop *EventLoop) {
		loop.ScheduleCallback(duration-time.Since(callTime), func() {
			fut.SetResult(nil, nil)
		})
	}).Await(ctx)
	return err
}

func Go[T any](ctx context.Context, f func(ctx context.Context) (T, error)) *Future[T] {
	loop := RunningLoop(ctx)
	fut := NewFuture[T]()

	goroCtx := context.WithValue(context.WithValue(ctx, runningLoop{}, nil), yielder{}, nil)
	go func() {
		result, err := f(goroCtx)
		loop.RunCallbackThreadsafe(func() {
			fut.SetResult(result, err)
		})
	}()
	return fut
}
