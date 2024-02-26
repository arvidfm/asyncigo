package asyncigo_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/arvidfm/asyncigo"
)

func Example() {
	_ = asyncigo.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
		task := asyncigo.SpawnTask(ctx, func(ctx context.Context) (int, error) {
			for i := range 3 {
				fmt.Printf("in subtask: %d\n", i)
				_ = asyncigo.Sleep(ctx, time.Second)
			}
			return 42, nil
		})

		for j := range 3 {
			fmt.Printf("in main task: %d\n", j)
			_ = asyncigo.Sleep(ctx, time.Second)
		}

		result, _ := task.Await(ctx)
		fmt.Printf("task result: %d\n", result)
		return nil
	})
	// Output:
	// in main task: 0
	// in subtask: 0
	// in main task: 1
	// in subtask: 1
	// in main task: 2
	// in subtask: 2
	// task result: 42
}

func ExampleSpawnTask() {
	_ = asyncigo.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
		var counter int
		var tasks []asyncigo.Futurer
		for range 100000 {
			tasks = append(tasks, asyncigo.SpawnTask(ctx, func(ctx context.Context) (any, error) {
				for range 10 {
					counter++
					_ = asyncigo.Sleep(ctx, time.Millisecond*500)
				}
				return nil, nil
			}))
		}

		_ = asyncigo.Wait(ctx, asyncigo.WaitAll, tasks...)

		fmt.Println(counter)
		return nil
	})
	// Output:
	// 1000000
}

func ExampleFuture_Shield() {
	_ = asyncigo.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
		shielded := asyncigo.NewFuture[any]()
		task1 := asyncigo.SpawnTask(ctx, func(ctx context.Context) (any, error) {
			fmt.Println("waiting for shielded...")
			return shielded.Shield().Await(ctx)
		})

		unshielded := asyncigo.NewFuture[any]()
		task2 := asyncigo.SpawnTask(ctx, func(ctx context.Context) (any, error) {
			fmt.Println("waiting for unshielded...")
			return unshielded.Await(ctx)
		})

		// yield to the event loop for one tick to initialise the tasks
		_ = asyncigo.RunningLoop(ctx).Yield(ctx, nil)

		task1.Cancel(nil)
		task2.Cancel(nil)

		fmt.Println("task1:", task1.Err())
		fmt.Println("task2:", task2.Err())
		fmt.Println("shielded:", shielded.Err())
		fmt.Println("unshielded:", unshielded.Err())
		return nil
	})
	// Output:
	// waiting for shielded...
	// waiting for unshielded...
	// task1: context canceled
	// task2: context canceled
	// shielded: <nil>
	// unshielded: context canceled
}

// When a task has been cancelled, it will continue running, but any calls to [Awaitable.Await]
// will immediately return [context.Canceled].
// It's the responsibility of the task to stop early when cancelled.
func ExampleTask_Cancel() {
	_ = asyncigo.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
		futs := make([]asyncigo.Future[int], 10)
		task := asyncigo.SpawnTask(ctx, func(ctx context.Context) (int, error) {
			for i := range futs {
				result, err := futs[i].Await(ctx)
				fmt.Printf("%d: (%v, %v)\n", i, result, err)
			}
			return 0, nil
		})

		loop := asyncigo.RunningLoop(ctx)
		for i := range futs {
			if i == 5 {
				task.Cancel(nil)
			}

			_ = loop.Yield(ctx, nil)
			futs[i].SetResult(i, nil)
		}

		result, err := task.Await(ctx)
		fmt.Printf("task result: (%v, %v)", result, err)
		return nil
	})
	// Output:
	// 0: (0, <nil>)
	// 1: (1, <nil>)
	// 2: (2, <nil>)
	// 3: (3, <nil>)
	// 4: (4, <nil>)
	// 5: (0, context canceled)
	// 6: (0, context canceled)
	// 7: (0, context canceled)
	// 8: (0, context canceled)
	// 9: (0, context canceled)
	// task result: (0, context canceled)
}

func ExampleIterator_Collect() {
	it := asyncigo.Iter(func(yield func(int) bool) {
		a, b := 0, 1
		for a < 20 {
			if !yield(a) {
				return
			}
			a, b = a+b, a
		}
	})

	fmt.Println(it.Collect())
	// Output:
	// [0 1 1 2 3 5 8 13]
}

func ExampleMapIterator_Collect() {
	it := asyncigo.MapIter(func(yield func(int, string) bool) {
		_ = yield(5, "a") &&
			yield(10, "b") &&
			yield(15, "c")
	})

	m := it.Collect()
	for k := range m {
		fmt.Printf("%d: %s\n", k, m[k])
	}
	// Unordered output:
	// 5: a
	// 10: b
	// 15: c
}

func ExampleRange() {
	it := asyncigo.Range(5)

	for i := range it {
		fmt.Println(i)
	}
	// Output:
	// 0
	// 1
	// 2
	// 3
	// 4
}

func ExampleZip() {
	it := asyncigo.Zip(
		asyncigo.AsIterator([]int{1, 2, 3, 4}),
		asyncigo.AsIterator([]string{"a", "b", "c", "d", "e"}),
	)

	for a, b := range it {
		fmt.Printf("%d: %s\n", a, b)
	}
	// Output:
	// 1: a
	// 2: b
	// 3: c
	// 4: d
}

func ExampleZipLongest() {
	it := asyncigo.ZipLongest(
		asyncigo.AsIterator([]int{1, 2, 3, 4}),
		asyncigo.AsIterator([]string{"a", "b", "c", "d", "e"}),
	)

	for a, b := range it {
		fmt.Printf("%d: %s\n", a, b)
	}
	// Output:
	// 1: a
	// 2: b
	// 3: c
	// 4: d
	// 0: e
}

func ExampleEnumerate() {
	it := asyncigo.Enumerate(5, asyncigo.AsIterator([]string{"a", "b", "c", "d"}))

	for i, v := range it {
		fmt.Printf("%d: %s\n", i, v)
	}
	// Output:
	// 5: a
	// 6: b
	// 7: c
	// 8: d
}

func ExampleFlatMap() {
	it := asyncigo.FlatMap(
		asyncigo.Range(5),
		func(v int) asyncigo.Iterator[int] {
			return asyncigo.Range(v + 1)
		},
	)

	fmt.Println(it.Collect())
	// Output:
	// [0 0 1 0 1 2 0 1 2 3 0 1 2 3 4]
}

func ExampleChain() {
	it := asyncigo.Chain(
		asyncigo.AsIterator([]int{1, 2, 3, 4}),
		asyncigo.AsIterator([]int{5, 6, 7}),
		asyncigo.AsIterator([]int{8, 9, 10, 11, 12}),
	)

	fmt.Println(it.Collect())
	// Output:
	// [1 2 3 4 5 6 7 8 9 10 11 12]
}

func ExampleFlatten() {
	it := asyncigo.Flatten(func(yield func(iterator asyncigo.Iterator[int]) bool) {
		_ = yield(asyncigo.AsIterator([]int{1, 2, 3, 4})) &&
			yield(asyncigo.AsIterator([]int{5, 6, 7})) &&
			yield(asyncigo.AsIterator([]int{8, 9, 10, 11, 12}))
	})

	fmt.Println(it.Collect())
	// Output:
	// [1 2 3 4 5 6 7 8 9 10 11 12]
}

func ExampleUniq() {
	it := asyncigo.Uniq(
		asyncigo.AsIterator([]int{1, 3, 5, 5, 8, 5, 2, 3, 9, 7, 3, 3, 4, 1}),
	)

	fmt.Println(it.Collect())
	// Output:
	// [1 3 5 8 2 9 7 4]
}

// This is a basic example showing how results and errors are yielded
// when iterating over an [AsyncIterable].
func ExampleAsyncIter_basic() {
	it := asyncigo.AsyncIter(func(yield func(int) error) error {
		for i := range 5 {
			if err := yield(i); err != nil {
				return err
			}
		}

		return errors.New("oops")
	})

	for i, err := range it {
		fmt.Printf("%d - %v\n", i, err)
	}
	// Output:
	// 0 - <nil>
	// 1 - <nil>
	// 2 - <nil>
	// 3 - <nil>
	// 4 - <nil>
	// 0 - oops
}

// Basic usage of UntilErr.
func ExampleAsyncIterable_UntilErr() {
	it := asyncigo.AsyncIter(func(yield func(int) error) error {
		for i := range 5 {
			if err := yield(i); err != nil {
				return err
			}
		}

		return errors.New("oops")
	})

	var err error
	for i := range it.UntilErr(&err) {
		fmt.Println(i)
	}
	fmt.Println(err)
	// Output:
	// 0
	// 1
	// 2
	// 3
	// 4
	// oops
}

// This example creates one future and two tasks and waits for them all to finish.
// Combining Wait with [asyncigo.Awaitable.WriteResultTo] allows for very succinct code.
//
// In this case, the future and one of the tasks succeed, while the second task fails with an error.
// We can see how the error from the failing task is propagated by Wait,
// while the results are written to the specified locations.
func ExampleWait() {
	_ = asyncigo.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
		fut1 := asyncigo.NewFuture[string]()
		task1 := asyncigo.SpawnTask(ctx, func(ctx context.Context) (int, error) {
			_ = asyncigo.Sleep(ctx, time.Second)

			fut1.SetResult("test", nil)

			return 20, nil
		})
		task2 := asyncigo.SpawnTask(ctx, func(ctx context.Context) (float64, error) {
			_ = asyncigo.Sleep(ctx, time.Second)

			return 25.5, errors.New("oops")
		})

		var result1 string
		var result2 int
		var result3 float64
		err := asyncigo.Wait(
			ctx,
			asyncigo.WaitAll,
			fut1.WriteResultTo(&result1),
			task1.WriteResultTo(&result2),
			task2.WriteResultTo(&result3),
		)

		fmt.Println("results:", result1, result2, result3)
		fmt.Println("error:", err)
		return nil
	})
	// Output:
	// results: test 20 25.5
	// error: oops
}

// As Go's implementation of coroutines doesn't require us to use an "await" keyword
// every time we call one, it's easy to write "polyglottal" functions that work both
// synchronously and asynchronously, making the "[coloured functions]" problem less of an issue.
//
// This example shows how one might write a sleep function that blocks
// if used outside an event loop, but not if an event loop is available.
//
// [coloured functions]: https://journal.stuffwithstuff.com/2015/02/01/what-color-is-your-function/
func Example_polyglot() {
	ctx := context.Background()
	_ = sleepPolyglot(ctx, time.Second*2)

	_ = asyncigo.NewEventLoop().Run(ctx, func(ctx context.Context) error {
		start := time.Now()

		numTasks := 100
		tasks := asyncigo.Map(asyncigo.Range(numTasks), func(int) asyncigo.Futurer {
			return asyncigo.SpawnTask(ctx, func(ctx context.Context) (any, error) {
				_ = sleepPolyglot(ctx, time.Second*2)
				return nil, nil
			})
		}).Collect()
		_ = asyncigo.Wait(ctx, asyncigo.WaitAll, tasks...)

		elapsed := time.Since(start)
		fmt.Printf("%d tasks took %d seconds to finish\n", len(tasks), int(math.Round(elapsed.Seconds())))

		return nil
	})
	// Output:
	// 100 tasks took 2 seconds to finish
}

func sleepPolyglot(ctx context.Context, duration time.Duration) error {
	if _, ok := asyncigo.RunningLoopMaybe(ctx); ok {
		return asyncigo.Sleep(ctx, duration)
	}
	time.Sleep(duration)
	return nil
}
