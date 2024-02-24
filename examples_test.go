package asyngio_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/arvidfm/asyngio"
	"time"
)

func ExampleSpawnTask() {
	asyngio.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
		var counter int
		var tasks []asyngio.Futurer
		for range 10000 {
			tasks = append(tasks, asyngio.SpawnTask(ctx, func(ctx context.Context) (any, error) {
				for range 100 {
					counter++
					asyngio.Sleep(ctx, time.Millisecond)
				}
				return nil, nil
			}))
		}

		asyngio.Wait(asyngio.WaitAll, tasks...).Await(ctx)

		fmt.Println(counter)
		return nil
	})
	// Output:
	// 1000000
}

func ExampleFuture_Shield() {
	asyngio.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
		shielded := asyngio.NewFuture[any]()
		task1 := asyngio.SpawnTask(ctx, func(ctx context.Context) (any, error) {
			fmt.Println("waiting for shielded...")
			return shielded.Shield().Await(ctx)
		})

		unshielded := asyngio.NewFuture[any]()
		task2 := asyngio.SpawnTask(ctx, func(ctx context.Context) (any, error) {
			fmt.Println("waiting for unshielded...")
			return unshielded.Await(ctx)
		})

		// yield to the event loop for one tick to initialise the tasks
		asyngio.RunningLoop(ctx).Yield(ctx, nil)

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

func ExampleIterator_Collect() {
	it := asyngio.Iter(func(yield func(int) bool) {
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
	it := asyngio.MapIter(func(yield func(int, string) bool) {
		_ = yield(5, "a") &&
			yield(10, "b") &&
			yield(15, "c")
	})

	var m map[int]string = it.Collect()

	for k, v := range m {
		fmt.Printf("%d: %s\n", k, v)
	}
	// Unordered output:
	// 5: a
	// 10: b
	// 15: c
}

func ExampleRange() {
	it := asyngio.Range(5)

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
	it := asyngio.Zip(
		asyngio.AsIterator([]int{1, 2, 3, 4}),
		asyngio.AsIterator([]string{"a", "b", "c", "d", "e"}),
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
	it := asyngio.ZipLongest(
		asyngio.AsIterator([]int{1, 2, 3, 4}),
		asyngio.AsIterator([]string{"a", "b", "c", "d", "e"}),
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
	it := asyngio.Enumerate(5, asyngio.AsIterator([]string{"a", "b", "c", "d"}))

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
	it := asyngio.FlatMap(
		asyngio.Range(5),
		func(v int) asyngio.Iterator[int] {
			return asyngio.Range(v + 1)
		},
	)

	fmt.Println(it.Collect())
	// Output:
	// [0 0 1 0 1 2 0 1 2 3 0 1 2 3 4]
}

func ExampleChain() {
	it := asyngio.Chain(
		asyngio.AsIterator([]int{1, 2, 3, 4}),
		asyngio.AsIterator([]int{5, 6, 7}),
		asyngio.AsIterator([]int{8, 9, 10, 11, 12}),
	)

	fmt.Println(it.Collect())
	// Output:
	// [1 2 3 4 5 6 7 8 9 10 11 12]
}

func ExampleFlatten() {
	it := asyngio.Flatten(func(yield func(iterator asyngio.Iterator[int]) bool) {
		_ = yield(asyngio.AsIterator([]int{1, 2, 3, 4})) &&
			yield(asyngio.AsIterator([]int{5, 6, 7})) &&
			yield(asyngio.AsIterator([]int{8, 9, 10, 11, 12}))
	})

	fmt.Println(it.Collect())
	// Output:
	// [1 2 3 4 5 6 7 8 9 10 11 12]
}

func ExampleUniq() {
	it := asyngio.Uniq(
		asyngio.AsIterator([]int{1, 3, 5, 5, 8, 5, 2, 3, 9, 7, 3, 3, 4, 1}),
	)

	fmt.Println(it.Collect())
	// Output:
	// [1 3 5 8 2 9 7 4]
}

func ExampleAsyncIter() {
	it := asyngio.AsyncIter(func(yield func(int) error) error {
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

func ExampleAsyncIterable_UntilErr() {
	it := asyngio.AsyncIter(func(yield func(int) error) error {
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
