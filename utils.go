package asyncigo

import (
	"context"
	"time"
)

// Queue provides a basic asynchronous queue.
// Queue is not threadsafe.
type Queue[T any] struct {
	data []T
	futs []*Future[T]
}

// Get pops the first item from the Queue.
// The returned [Future] will resolve to the popped item
// once data is available.
func (q *Queue[T]) Get() *Future[T] {
	fut := NewFuture[T]()
	if len(q.data) > 0 {
		item := q.data[0]
		q.data = q.data[1:]
		fut.SetResult(item, nil)
		return fut
	}

	q.futs = append(q.futs, fut)
	return fut
}

// Push adds an item to the Queue.
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

// Mutex provides a simple asynchronous locking mechanism for coroutines.
// Mutex is not threadsafe.
type Mutex struct {
	unlockFut *Future[any]
}

// Lock locks the Mutex. If the Mutex is already locked,
// the calling coroutine will be suspended until unlocked.
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

// Unlock unlocks the Mutex.
func (m *Mutex) Unlock() {
	if m.unlockFut != nil {
		m.unlockFut.SetResult(nil, nil)
	}
}

// WaitMode modifies the behaviour of [Wait].
type WaitMode int

const (
	WaitFirstResult WaitMode = iota // wait until any future has a result or an error
	WaitFirstError                  // wait until any future has an error or until all futures have completed
	WaitAll                         // wait until all futures have completed or errored
)

// Wait will wait for any or all of the given Futures to complete
// depending on the [WaitMode] passed.
// Wait will not cancel any futures.
func Wait(mode WaitMode, futs ...Futurer) *Future[any] {
	var done int
	var futErr error
	waitFut := NewFuture[any]()

	for _, fut := range futs {
		fut.AddDoneCallback(func(err error) {
			done++
			if err != nil {
				futErr = err
				if mode != WaitAll || done >= len(futs) {
					waitFut.SetResult(nil, err)
				}
			} else if done >= len(futs) || mode == WaitFirstResult {
				waitFut.SetResult(nil, futErr)
			}
		})
	}
	return waitFut
}

// GetFirstResult returns the result of the first successful coroutine.
// Once a coroutine succeeds, all unfinished tasks will be cancelled.
// If no coroutine succeeds, the last error is returned.
func GetFirstResult[T any](ctx context.Context, coros ...Coroutine2[T]) (T, error) {
	taskCtx, cancel := context.WithCancel(ctx)
	tasks := make([]*Task[T], 0, len(coros))

	var done int
	waitFut := NewFuture[T]()
	waitFut.AddResultCallback(func(_ T, err error) {
		// prevent new tasks from spawning
		cancel()
		// cancel any already started tasks
		for _, t := range tasks {
			t.Cancel(nil)
		}
	})

	for i, coro := range coros {
		tasks = append(tasks, SpawnTask(taskCtx, coro))
		tasks[i].AddResultCallback(func(result T, err error) {
			done++
			if err == nil {
				waitFut.SetResult(result, nil)
			} else if done >= len(coros) {
				waitFut.Cancel(err)
			}
		})
	}

	return waitFut.Await(ctx)
}

// Sleep suspends the current coroutine for the given duration.
func Sleep(ctx context.Context, duration time.Duration) error {
	fut := NewFuture[any]()
	handle := RunningLoop(ctx).ScheduleCallback(duration, func() {
		fut.SetResult(nil, nil)
	})
	fut.AddDoneCallback(func(err error) {
		handle.Cancel()
	})
	_, err := fut.Await(ctx)
	return err
}

// Go launches the given function in a goroutine and returns a [Future]
// that will complete when the goroutine finishes.
func Go[T any](ctx context.Context, f func(ctx context.Context) (T, error)) *Future[T] {
	loop := RunningLoop(ctx)
	fut := NewFuture[T]()

	goroCtx := context.WithValue(ctx, runningLoop{}, nil)
	go func() {
		result, err := f(goroCtx)
		loop.RunCallbackThreadsafe(goroCtx, func() {
			fut.SetResult(result, err)
		})
	}()
	return fut
}
