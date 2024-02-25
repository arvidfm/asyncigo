package asyncigo

import (
	"context"
	"errors"
	"iter"
)

var (
	ErrNotReady = errors.New("future is still pending")
)

// Coroutine1 is a coroutine that can return an error.
type Coroutine1 func(ctx context.Context) error

// SpawnTask is a convenience function for starting this coroutine as a background task.
func (c Coroutine1) SpawnTask(ctx context.Context) *Task[any] {
	return SpawnTask[any](ctx, func(ctx context.Context) (any, error) {
		return nil, c(ctx)
	})
}

// Coroutine2 is a coroutine that can return a result or an error.
type Coroutine2[R any] func(ctx context.Context) (R, error)

// SpawnTask is a convenience function for starting this coroutine as a background task.
func (c Coroutine2[R]) SpawnTask(ctx context.Context) *Task[R] {
	return SpawnTask(ctx, c)
}

// Futurer is an untyped view of an [Awaitable], useful for storing
// heterogeneous Awaitable instances in a container.
type Futurer interface {
	// HasResult reports whether this Futurer has completed.
	// This is true if the Futurer has a result, or if it has been cancelled.
	HasResult() bool
	// Err returns a non-nil error if it has been cancelled
	// or completed with an error.
	Err() error
	// AddDoneCallback registers a type-unaware callback to run once this Futurer
	// completes or is cancelled. If called when the Futurer has already completed,
	// the callback will be run immediately.
	AddDoneCallback(callback func(error)) Futurer
	// Cancel cancels this Futurer. If err is nil, the Futurer
	// will be canceled with [context.Canceled]. If the Futurer
	// has already completed, this has no effect.
	Cancel(err error)
}

// tasker is an untyped view of a [Task].
type tasker interface {
	Futurer
	yield(ctx context.Context, fut Futurer) error
}

// Awaitable is a type that holds the result of an operation
// that may complete at a later point in time, and which can be
// awaited to suspend the current coroutine until the operation
// has completed.
//
// This interface enables polymorphism for [Future] and [Task] objects.
type Awaitable[T any] interface {
	Futurer
	// Await suspends the current task until this [Awaitable]
	// has completed and returns its result once completed.
	//
	// If the calling [Task] or the given [context.Context]
	// is cancelled before Await completes, an error will be returned
	// and the Awaitable will be cancelled as well.
	// See the [Awaitable.Shield] method if you do not want the Awaitable to be cancelled.
	Await(ctx context.Context) (T, error)
	// MustAwait is the same as [Awaitable.Await], but it panics if Await
	// returns an error.
	MustAwait(ctx context.Context) T
	// Shield returns a new [Future] which completes once this Awaitable completes,
	// but which will not cancel this Awaitable if cancelled.
	// Allows for awaiting an Awaitable from a [Task] without cancelling
	// the Awaitable if the Task is cancelled.
	Shield() *Future[T]
	// AddResultCallback registers a type-aware callback to run once this Awaitable
	// completes or is cancelled. If called when the Awaitable has already completed,
	// the callback will be run immediately.
	AddResultCallback(callback func(result T, err error)) Awaitable[T]
	// WriteResultTo registers a pointer to write the result
	// of this Awaitable to if it completes with no error.
	//
	// This method allows for particularly ergonomic use
	// of functions like [Wait].
	WriteResultTo(dst *T) Awaitable[T]
	// Future returns the underlying [Future] holding the result
	// of this Awaitable. Returns itself if the Awaitable is a Future.
	Future() *Future[T]
	// Result returns the result of this Awaitable.
	// If this Awaitable has not yet completed, [ErrNotReady] will be returned.
	Result() (T, error)
}

// Future is a value container representing the result of a pending operation.
// It will run any callbacks registered using [Futurer.AddDoneCallback] or [Awaitable.AddResultCallback]
// once populated with a result using either [Future.SetResult] or [Futurer.Cancel].
type Future[ResType any] struct {
	done      bool
	result    ResType
	err       error
	callbacks []func(ResType, error)
}

// NewFuture returns a new [Future] instance ready to be awaited
// or populated with a result.
func NewFuture[ResType any]() *Future[ResType] {
	return &Future[ResType]{}
}

// HasResult implements [Futurer].
func (f *Future[ResType]) HasResult() bool {
	return f.done
}

// Err implements [Futurer].
func (f *Future[ResType]) Err() error {
	return f.err
}

// Result implements [Awaitable].
func (f *Future[ResType]) Result() (ResType, error) {
	if f.done {
		return f.result, f.err
	}

	var zero ResType
	return zero, ErrNotReady
}

// Future implements [Awaitable].
func (f *Future[ResType]) Future() *Future[ResType] {
	return f
}

// AddDoneCallback implements [Futurer].
func (f *Future[ResType]) AddDoneCallback(callback func(error)) Futurer {
	f.AddResultCallback(func(_ ResType, err error) {
		callback(err)
	})
	return f
}

// AddResultCallback implements [Awaitable].
func (f *Future[ResType]) AddResultCallback(callback func(ResType, error)) Awaitable[ResType] {
	if f.HasResult() {
		callback(f.result, f.err)
	} else {
		f.callbacks = append(f.callbacks, callback)
	}
	return f
}

// WriteResultTo implements [Awaitable].
func (f *Future[ResType]) WriteResultTo(dest *ResType) Awaitable[ResType] {
	return f.AddResultCallback(func(result ResType, err error) {
		if err == nil {
			*dest = result
		}
	})
}

// Await implements [Awaitable].
func (f *Future[ResType]) Await(ctx context.Context) (ResType, error) {
	if err := RunningLoop(ctx).Yield(ctx, f); err != nil {
		var zero ResType
		return zero, err
	}
	return f.Result()
}

// MustAwait implements [Awaitable].
func (f *Future[ResType]) MustAwait(ctx context.Context) ResType {
	res, err := f.Await(ctx)
	if err != nil {
		panic(err)
	}
	return res
}

// Cancel implements [Futurer].
func (f *Future[ResType]) Cancel(err error) {
	if err == nil {
		err = context.Canceled
	}
	var zero ResType
	f.SetResult(zero, err)
}

// Shield implements [Awaitable].
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

// SetResult populates this Future with a result.
// This will mark the Future as completed, and the provided result
// will be propagated to any registered callbacks
// and returned from any future calls to [Awaitable.Result].
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

// Task is responsible for driving a coroutine, intercepting any [Awaitable] instances
// awaited from the coroutine and advancing the coroutine once the pending Awaitable completes.
type Task[RetType any] struct {
	loop    *EventLoop
	yielder func(Futurer) bool

	next       func() (Futurer, bool)
	stop       func()
	ctx        context.Context
	cancel     context.CancelCauseFunc
	pendingFut Futurer
	resultFut  *Future[RetType]
}

// SpawnTask starts the given coroutine as a background task.
func SpawnTask[RetType any](ctx context.Context, coro Coroutine2[RetType]) *Task[RetType] {
	ctx, cancel := context.WithCancelCause(ctx)
	task := &Task[RetType]{
		loop:      RunningLoop(ctx),
		resultFut: NewFuture[RetType](),
		ctx:       ctx,
		cancel:    cancel,
	}

	// this is where the magic happens; the entirety of the library
	// is predicated on this iter.Pull call
	next, stop := iter.Pull(func(yield func(Futurer) bool) {
		task.yielder = yield
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

	// defer the first call to step() to after control
	// has been handed back to the event loop
	// so the task can't finish before SpawnTask returns
	// (ensures that early cancelling of tasks prevents
	// them from running at all)
	task.loop.RunCallback(func() {
		// don't start the coroutine if it or the context has already been cancelled
		if task.resultFut.HasResult() {
			return
		} else if err := context.Cause(ctx); err != nil {
			task.resultFut.Cancel(err)
		} else {
			task.step()
		}
	})
	return task
}

// step advances the coroutine until its subsequent call to
// [Awaitable.Await] or [EventLoop.Yield].
func (t *Task[_]) step() (ok bool) {
	// tell the event loop what the currently running task is
	// (needed for EventLoop.Yield to work!)
	t.loop.withTask(t, func() {
		t.pendingFut, ok = t.next()
	})
	if ok {
		if t.pendingFut != nil {
			t.pendingFut.AddDoneCallback(func(err error) {
				t.step()
			})
		} else {
			// if a nil future was yielded, treat it as a signal
			// to yield to the event loop for one tick
			t.loop.RunCallback(func() {
				t.step()
			})
		}
		return true
	} else {
		t.pendingFut = nil
		t.stop()
		return false
	}
}

// Stop aborts the coroutine, preventing any further awaits.
// You should generally use [Futurer.Cancel] instead.
func (t *Task[_]) Stop() {
	t.stop()
}

func (t *Task[_]) yield(childCtx context.Context, fut Futurer) error {
	// this is the most delicate part of the code;
	// any changes here will affect the error handling
	// and cancellation semantics of all tasks

	// cancel the task if the task context has been cancelled
	if err := context.Cause(t.ctx); err != nil {
		t.resultFut.Cancel(err)
		if fut != nil {
			fut.Cancel(err)
		}
		return t.Err()
	}

	// cancel the task if the caller context has been cancelled
	if err := childCtx.Err(); err != nil {
		if fut != nil {
			fut.Cancel(err)
		}
		return t.Err()
	}

	// suspend the coroutine, passing the future to Task.step
	if !t.yielder(fut) {
		t.resultFut.Cancel(nil)
		return t.Err()
	}

	// check again if the contexts were cancelled while
	// the coroutine was suspended
	// the yielded future will have completed here,
	// so no point in cancelling it
	if err := context.Cause(t.ctx); err != nil {
		t.resultFut.Cancel(err)
		return t.Err()
	}
	if err := childCtx.Err(); err != nil {
		t.resultFut.Cancel(err)
		return t.Err()
	}
	return nil
}

// HasResult implements [Futurer].
func (t *Task[_]) HasResult() bool {
	return t.resultFut.HasResult()
}

// Result implements [Awaitable].
func (t *Task[RetType]) Result() (RetType, error) {
	return t.resultFut.Result()
}

// Err implements [Futurer].
func (t *Task[_]) Err() error {
	return t.resultFut.Err()
}

// Future implements [Awaitable].
func (t *Task[RetType]) Future() *Future[RetType] {
	return t.resultFut
}

// Await implements [Awaitable].
func (t *Task[RetType]) Await(ctx context.Context) (RetType, error) {
	return t.resultFut.Await(ctx)
}

// MustAwait implements [Awaitable].
func (t *Task[RetType]) MustAwait(ctx context.Context) RetType {
	return t.resultFut.MustAwait(ctx)
}

// Shield implements [Awaitable].
func (t *Task[RetType]) Shield() *Future[RetType] {
	return t.resultFut.Shield()
}

// WriteResultTo implements [Awaitable].
func (t *Task[RetType]) WriteResultTo(dst *RetType) Awaitable[RetType] {
	t.resultFut.WriteResultTo(dst)
	return t
}

// Cancel implements [Futurer].
func (t *Task[_]) Cancel(err error) {
	t.resultFut.Cancel(err)
}

// AddResultCallback implements [Awaitable].
func (t *Task[RetType]) AddResultCallback(callback func(result RetType, err error)) Awaitable[RetType] {
	t.resultFut.AddResultCallback(callback)
	return t
}

// AddDoneCallback implements [Futurer].
func (t *Task[_]) AddDoneCallback(callback func(error)) Futurer {
	t.resultFut.AddDoneCallback(callback)
	return t
}
