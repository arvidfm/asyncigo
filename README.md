# asyncigo: asyncio-style event loops for Go

[![tag](https://img.shields.io/github/tag/arvidfm/asyncigo.svg)](https://github.com/arvidfm/asyncigo/releases)
[![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.22-%23007d9c)](https://go.dev/)
[![Go Reference](https://pkg.go.dev/badge/github.com/arvidfm/asyncigo.svg)](https://pkg.go.dev/github.com/arvidfm/asyncigo)
[![Build Status](https://github.com/arvidfm/asyncigo/actions/workflows/test.yml/badge.svg)](https://github.com/arvidfm/asyncigo/actions)
[![GitHub License](https://img.shields.io/github/license/arvidfm/asyncigo)](./LICENSE.txt)

asyncigo is a proof of concept framework for doing event loop-based asynchronous I/O in Go, modelled after Python's `asyncio`.

asyncigo comes with:

* **Tasks** that suspend and awake coroutines in response to I/O events
* **Futures** that tasks can use to wait for asynchronous results
* **Await** which lets you write asynchronous code that looks synchronous
* **TCP client sockets** for reading and writing data asynchronously over the internet
* **Asynchronous iterators** for ergonomically iterating over I/O streams
* **Iterator utilities** for mapping, filtering, chaining and otherwise manipulating functional iterators

## What?

asyncigo allows for large-scale[^1], single-threaded[^2] asynchronous I/O[^3] using asyncio-style, event loop-based coroutines, tasks and futures.
The cooperative nature of the concurrency model makes your code easier to reason about and reduces the need for synchronisation primitives, while still being capable of managing many thousands of I/O-bound tasks simultaneously.

[^1]: Not actually tested.
[^2]: Technically, each coroutine is run in its own goroutine which may be scheduled on any thread, but no two goroutines belonging to the same event loop will ever run at the same time, resulting in effectively single-threaded behaviour.
[^3]: The only actual asynchronous I/O currently supported is network sockets, and only on platforms that support `epoll` (i.e. Linux).

## How?

asyncigo is based on the [iterator functions](https://go.dev/wiki/RangefuncExperiment) that were added experimentally in Go 1.22.
In particular it ~~ab~~uses the fact that `iter.Pull` works by context switching between the iterator function and the calling function on each call to the `yield` and `next` functions, which can be used to emulate Python's `yield` and `send` with the help of some extra bookkeeping to track the results of the yielded futures.

## Why?

I thought it would be funny.
Give event loop fanatics an inch etc.
(It's me, hi, I'm the event loop fanatic.)

## OK, but seriously, why?

A single-threaded event loop model with cooperative concurrency has a number of advantages over traditional multithreading for I/O-bound tasks:

1. Less risk of race conditions.
2. Less need for manual synchronisation.
3. It's easier to reason about the code, as you know state can't change between await points.
4. Coroutines can have a lower footprint than threads.

In the context of Go, however, goroutines (being user-space green threads) already provide some of the same benefits as coroutines compared to OS threads, including faster context switching and a lower memory footprint.
Still, Go's preemptive model can be hard to work with if you're not careful, and if you're not generally bound by the CPU, it can be nice to have the guarantees that cooperative concurrency brings.

## Should I use it?

Probably not!
Go prides itself on having a single concurrency model built right into the language.
A library like this undermines that by introducing a parallel model, which risks fragmenting the language.
That said, there are clear reasons why one might prefer the event loop model, so use your own judgement.

Besides, this library is just a proof of concept, is very bare-bones, has not been tested in practice, and is currently only really useful on Linux.
I take no responsibility for any bricked computers, burnt-down houses, escaped pets, loss of sense of self, natural disasters or general feelings of discomfort that follow from the use of asyncigo.

If you do want to use this as a base for your own library, though, be my guest.
You could also simply use it as inspiration for designing more ergonomic goroutine-based frameworks.

## How do I use it?

Did you even read the previous section?
But OK, fine.

1. Fetch the module:
   ```
   go get github.com/arvidfm/asyncigo@latest
   ```
2. Make sure that you're compiling with the `rangefunc` experiment enabled:
   ```
   GOEXPERIMENT=rangefunc go build
   ```
   You may also need to enable the `goexperiment.rangefunc` build tag for your IDE to resolve the `iter` import correctly.

### Tasks

You can create and await tasks which will be run in parallel:

```go
asyncigo.NewEventLoop().Run(context.Background(), func(ctx context.Context), error {
    task := asyncigo.SpawnTask(ctx, func (ctx context.Context) (int, error) {
        for i := range 3 {
            fmt.Printf("in subtask: %d\n", i)
            Sleep(ctx, time.Second)
        }
        return 42, nil
    })
    
    for j := range 3 {
        fmt.Printf("in main task: %d\n", j)
        Sleep(ctx, time.Second)
    }

    task.Await(ctx)
})
// Output:
// in main task: 0
// in subtask: 0
// in main task: 1
// in subtask: 1
// in main task: 2
// in subtask: 2
// task result: 42
```

You can wait for multiple tasks at the same time:

```go
asyncigo.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
    fut1 := asyncigo.NewFuture[string]()
 
    task1 := asyncigo.SpawnTask(ctx, func(ctx context.Context) (int, error) {
        asyncigo.Sleep(ctx, time.Second)
        fut1.SetResult("test", nil)
        return 20, nil
    })
 
    task2 := asyncigo.SpawnTask(ctx, func(ctx context.Context) (float64, error) {
        asyncigo.Sleep(ctx, time.Second)
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
```

### Asynchronous iterators

asyncigo supports asynchronous iterator functions that let you wait for asynchronous I/O events while also progressively yielding results, similar to `async` generators in Python.
These can then be easily ranged over.

For instance, you can read chunks of data from a socket, process each chunk, and then yield the result:

```go
if err := asyncigo.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
    it := asyncigo.AsyncIter(func(yield func(int) error) error {
        stream, _ := asyncigo.RunningLoop(ctx).Dial(ctx, "tcp", "localhost:6172")

        for {
            line, err := stream.ReadLine(ctx)
            if errors.Is(err, io.EOF) {
                return nil
            } else if err != nil {
                return err
            }

            unicode := []rune(strings.TrimSpace(string(line)))
            _ = yield(len(unicode))
        }
    })

    for lineLength, err := range it {
        if err != nil {
            return err
        }

        fmt.Println(lineLength)
    }
    return nil
}); err != nil {
    panic(err)
}
// Output:
// 6
// 12
// 18
// 30
```

Using `UntilErr`, we can iterate over asynchronous iterators a bit more ergonomically, and also combine them with other utilities that work with function iterators like `Map`, `Filter` or `Chain`:

```go
if err := asyncigo.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
    loop := asyncigo.RunningLoop(ctx)

    var err error
    for line := range asyncigo.Chain(
        loop.DialLines(ctx, "tcp", "localhost:6172").UntilErr(&err),
        loop.DialLines(ctx, "tcp", "localhost:6173").UntilErr(&err),
    ) {
        fmt.Printf("got line: %s", line)
    }

    return err
}); err != nil {
    panic(err)
}
// Output:
// got line: Lorem ipsum dolor sit amet.
// got line: Donec non velit consequat.
// got line: Donec interdum in nulla ac scelerisque.
// got line: Duis commodo, neque ac luctus eleifend.
// got line: Fusce lacinia id quam ac porttitor.
// got line: 生麦生米生卵
// got line: すもももももももものうち
// got line: 東京特許許可局長今日急遽休暇許可却下
// got line: 斜め77度の並びで泣く泣く嘶くナナハン7台難なく並べて長眺め
```

### Task cancellation

The cancellation semantics are not yet finalised, particularly regarding to what extent a task should have the opportunity to recover or clean up following cancellation.
At the moment, the coroutine itself will continue running after the task has been cancelled, but its context will be cancelled, and any further calls to `Await` will immediately return `context.Canceled`:

```go
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
```

For now, it is thus the responsibility of the task itself to exit early on cancellation, but any further asynchronous operations will be ignored.

Additionally, cancelling a task will also cancel any future or task it's currently awaiting.
If you want to prevent an awaited future from being cancelled, use `Shield`:

```go
fut.Shield().Await(ctx)
```

### Polyglottal functions

Go has no language-level distinction between coroutines and normal functions, making it possible to write functions that work both synchronously and asynchronously.
For example, you could write a polyglottal sleep function as so:

```go
func SleepPolyglot(ctx context.Context, duration time.Duration) error {
    if _, ok := asyncigo.RunningLoopMaybe(ctx); ok {
        return asyncigo.Sleep(ctx, duration)
    }
    time.Sleep(duration)
    return nil
}
```
