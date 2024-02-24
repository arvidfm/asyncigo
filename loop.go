package asyngio

import (
	"container/heap"
	"context"
	"log/slog"
	"time"
)

type runningLoop struct{}

// RunningLoop returns the [EventLoop] running in the current context.
// If no EventLoop is running, this function will panic.
// This function should not generally be called from a manually launched goroutine.
func RunningLoop(ctx context.Context) *EventLoop {
	return ctx.Value(runningLoop{}).(*EventLoop)
}

// EventLoop implements the core mechanism for processing callbacks and I/O events.
type EventLoop struct {
	pendingCallbacks    callbackQueue
	callbacksFromThread chan *Callback
	callbacksDoneFut    *Future[any]

	poller       Poller
	currentTasks []tasker
}

// NewEventLoop constructs a new [EventLoop].
func NewEventLoop() *EventLoop {
	return &EventLoop{
		callbacksFromThread: make(chan *Callback, 100),
	}
}

// Run starts the event loop with the given coroutine as the main task.
// The loop will exit once the main task has exited and there are no pending callbacks.
func (e *EventLoop) Run(ctx context.Context, main Coroutine1) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	var err error
	if e.poller, err = NewPoller(); err != nil {
		return err
	}
	defer e.poller.Close()

	ctx = context.WithValue(ctx, runningLoop{}, e)
	mainTask := main.SpawnTask(ctx).Future().AddDoneCallback(func(err error) {
		if err != nil {
			cancel(err)
		}
	})

	for ctx.Err() == nil {
		e.addCallbacksFromThread(ctx)
		e.runReadyCallbacks(ctx)

		if e.callbacksDoneFut != nil && e.pendingCallbacks.Empty() {
			e.callbacksDoneFut.SetResult(nil, nil)
			e.callbacksDoneFut = nil
			continue
		}

		if ctx.Err() != nil || (mainTask.HasResult() && e.pendingCallbacks.Empty()) {
			break
		}

		timeout := time.Second * 30
		if !e.pendingCallbacks.Empty() {
			timeout = e.pendingCallbacks.TimeUntilNext()
		}
		if deadline, ok := ctx.Deadline(); ok {
			untilDeadline := time.Until(deadline)
			if untilDeadline < timeout {
				timeout = untilDeadline
			}
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
	for ctx.Err() == nil && e.pendingCallbacks.RunNext() {
	}
}

// withTask pushes the currently executing task to the top of the task stack
// so that [EventLoop.Yield] knows what task's yielder to use.
func (e *EventLoop) withTask(t tasker, step func()) {
	oldTasks := e.currentTasks
	e.currentTasks = append(e.currentTasks, t)

	step()

	if e.currentTask() != t {
		panic("context switched from unexpected task")
	}
	e.currentTasks = oldTasks
}

func (e *EventLoop) currentTask() tasker {
	return e.currentTasks[len(e.currentTasks)-1]
}

// Yield yields control to the event loop for one tick, allowing pending callbacks and I/O to be processed.
func (e *EventLoop) Yield(ctx context.Context, fut Futurer) error {
	return e.currentTask().yield(ctx, fut)
}

// ScheduleCallback schedules a callback to be executed after the given duration.
func (e *EventLoop) ScheduleCallback(delay time.Duration, callback func()) *Callback {
	handle := NewCallback(delay, callback)
	e.pendingCallbacks.Add(handle)
	return handle
}

// RunCallback schedules a callback for immediate execution by the event loop.
// Not threadsafe; use [EventLoop.RunCallbackThreadsafe] to schedule callbacks from other threads.
func (e *EventLoop) RunCallback(callback func()) {
	e.ScheduleCallback(0, callback)
}

// RunCallbackThreadsafe schedules a callback for immediate execution on the event loop's thread.
func (e *EventLoop) RunCallbackThreadsafe(ctx context.Context, callback func()) {
	e.callbacksFromThread <- NewCallback(0, callback)
	if e.poller != nil {
		if err := e.poller.WakeupThreadsafe(); err != nil {
			slog.WarnContext(ctx, "could not wake up event loop from thread", slog.Any("error", err))
		}
	}
}

// WaitForCallbacks returns a [Future] that will complete once there are no pending callback functions.
func (e *EventLoop) WaitForCallbacks() *Future[any] {
	if e.callbacksDoneFut == nil {
		e.callbacksDoneFut = NewFuture[any]()
	}
	return e.callbacksDoneFut
}

// Pipe creates two streams, where writing to w will make the written data available from r.
func (e *EventLoop) Pipe() (r, w *AsyncStream, err error) {
	rf, wf, err := e.poller.Pipe()
	if err != nil {
		return nil, nil, err
	}

	return NewAsyncStream(rf), NewAsyncStream(wf), nil
}

// Dial opens a new network connection.
func (e *EventLoop) Dial(ctx context.Context, network, address string) (*AsyncStream, error) {
	f, err := e.poller.Dial(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return NewAsyncStream(f), nil
}

// Callback is a handle to a callback scheduled to be run by an [EventLoop].
type Callback struct {
	callback func()
	when     time.Time

	// queue == nil && index < 0 if the callback has not been scheduled
	// or has already run
	queue *callbackQueue
	index int
}

// NewCallback creates a new handle to a callback specified to run after the given amount of time.
//
// Calling this function will not actually schedule the callback to be run.
// Use EventLoop.ScheduleCallback instead.
func NewCallback(duration time.Duration, callback func()) *Callback {
	return &Callback{
		callback: callback,
		when:     time.Now().Add(duration),
		index:    -2,
	}
}

// Cancel removes this callback from its callback queue, preventing it from being run.
// If this callback is not currently scheduled, this method is a no-op and will return false.
func (c *Callback) Cancel() bool {
	if c.queue != nil {
		return c.queue.Remove(c)
	}
	return false
}

// callbackQueue is a priority queue of callbacks
// sorted so the topmost callback is the one scheduled to run the soonest.
type callbackQueue []*Callback

// Len implements [heap.Interface].
func (r *callbackQueue) Len() int {
	return len(*r)
}

// Less implements [heap.Interface].
func (r *callbackQueue) Less(i, j int) bool {
	return (*r)[i].when.Before((*r)[j].when)
}

// Swap implements [heap.Interface].
func (r *callbackQueue) Swap(i, j int) {
	(*r)[i].index = j
	(*r)[j].index = i
	(*r)[i], (*r)[j] = (*r)[j], (*r)[i]
}

// Push implements [heap.Interface].
func (r *callbackQueue) Push(x any) {
	callback := x.(*Callback)
	callback.index = r.Len()
	callback.queue = r
	*r = append(*r, callback)
}

// Pop implements [heap.Interface].
func (r *callbackQueue) Pop() (v any) {
	n := len(*r)
	callback := (*r)[n-1]
	*r = (*r)[:n-1]
	// remove association to the queue
	// so Remove behaves correctly if called multiple times
	callback.index = -1
	callback.queue = nil
	return v
}

// Remove removes a callback from the queue, effectively cancelling it.
// Returns true if the callback was successfully removed,
// and false if the callback is not in this queue.
func (r *callbackQueue) Remove(callback *Callback) bool {
	if callback.queue == nil || callback.queue != r || callback.index < 0 {
		return false
	}
	heap.Remove(r, callback.index)
	return true
}

// Peek returns the next callback to run without modifying the queue.
// Will panic if the queue is empty.
func (r *callbackQueue) Peek() *Callback {
	return (*r)[0]
}

// Add adds a new callback to the queue.
func (r *callbackQueue) Add(c *Callback) {
	heap.Push(r, c)
}

// RunNext runs the topmost callback in the queue.
// Returns false if there are no callbacks pending
// or the topmost callback is not due.
func (r *callbackQueue) RunNext() bool {
	if r.Empty() || r.TimeUntilNext() > 0 {
		return false
	}

	head := r.Peek()
	heap.Pop(r)
	head.callback()
	return true
}

// TimeUntilNext returns the time until the topmost callback in the queue is scheduled to run.
func (r *callbackQueue) TimeUntilNext() time.Duration {
	return time.Until(r.Peek().when)
}

// Empty reports whether the queue is empty.
func (r *callbackQueue) Empty() bool {
	return r.Len() == 0
}
