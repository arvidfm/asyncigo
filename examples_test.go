package asyncigo_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/arvidfm/asyncigo"
)

func ExampleSpawnTask() {
	asyncigo.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
		var counter int
		var tasks []asyncigo.Futurer
		for range 10000 {
			tasks = append(tasks, asyncigo.SpawnTask(ctx, func(ctx context.Context) (any, error) {
				for range 100 {
					counter++
					asyncigo.Sleep(ctx, time.Millisecond)
				}
				return nil, nil
			}))
		}

		asyncigo.Wait(asyncigo.WaitAll, tasks...).Await(ctx)

		fmt.Println(counter)
		return nil
	})
	// Output:
	// 1000000
}

func ExampleFuture_Shield() {
	asyncigo.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
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
		asyncigo.RunningLoop(ctx).Yield(ctx, nil)

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

func ExampleAsyncIter() {
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

func ExampleWait() {
	asyncigo.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
		fut1 := asyncigo.NewFuture[string]()
		task1 := asyncigo.SpawnTask(ctx, func(ctx context.Context) (int, error) {
			fut1.SetResult("test", nil)
			return 20, nil
		})
		task2 := asyncigo.SpawnTask(ctx, func(ctx context.Context) (float64, error) {
			return 25.5, errors.New("oops")
		})

		var result1 string
		var result2 int
		var result3 float64
		asyncigo.Wait(
			asyncigo.WaitAll,
			fut1.WriteResultTo(&result1),
			task1.WriteResultTo(&result2),
			task2.WriteResultTo(&result3),
		).Await(ctx)

		fmt.Println(result1, result2, result3)
		return nil
	})
	// Output:
	// test 20 0
}
