package asyncio_go

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSpawnTask(t *testing.T) {
	ctx := context.Background()
	err := NewEventLoop().Run(ctx, func(ctx context.Context) error {
		fut1 := SpawnTask(ctx, func(ctx context.Context) (int, error) {
			for i := range 5 {
				fmt.Println("in sync task", i)
				time.Sleep(time.Second)
			}
			return 11, nil
		})

		fut2 := SpawnTask(ctx, func(ctx context.Context) (int, error) {
			for i := range 5 {
				fmt.Println("in async task", i)
				Sleep(ctx, time.Second)
			}
			return 22, nil
		})

		Sleep(ctx, time.Second/2)

		for i := range 5 {
			fmt.Println("in coroutine", i)
			Sleep(ctx, time.Second)
		}

		fmt.Println("got results:",
			fut1.MustAwait(ctx),
			fut2.MustAwait(ctx))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGo(t *testing.T) {
	loop := NewEventLoop()
	err := loop.Run(context.Background(), func(ctx context.Context) error {
		t1 := time.Now()
		s1 := Go(ctx, func(ctx context.Context) (string, error) {
			time.Sleep(time.Second * 1)
			return "hello", nil
		}).MustAwait(ctx)
		fmt.Println("got result", s1, "after", time.Since(t1))

		t2 := time.Now()
		s2 := Go(ctx, func(ctx context.Context) (string, error) {
			time.Sleep(time.Second * 1)
			return "test", nil
		}).MustAwait(ctx)
		fmt.Println("got result", s2, "after", time.Since(t2))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPipe(t *testing.T) {
	loop := NewEventLoop()
	err := loop.Run(context.Background(), func(ctx context.Context) error {
		r, w, err := loop.Pipe()
		if err != nil {
			return err
		}

		writer := SpawnTask(ctx, func(ctx context.Context) (string, error) {
			defer w.Close()

			for i := range 5 {
				fmt.Println("writing lines", i*2, "and", i*2+1)
				w.Write(ctx, []byte(fmt.Sprintf("this is line %d\nand this is line %d\n", i*2, i*2+1)))
				Sleep(ctx, time.Second)
			}
			return "", nil
		})

		reader := SpawnTask(ctx, func(ctx context.Context) (string, error) {
			defer r.Close()

			for line, err := range r.Lines(ctx) {
				if err != nil {
					return "", err
				}
				fmt.Println("got line:", string(line))
			}
			fmt.Println("reader exited")
			return "", nil
		})

		writer.MustAwait(ctx)
		reader.MustAwait(ctx)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestEventLoop_Dial(t *testing.T) {
	loop := NewEventLoop()
	err := loop.Run(context.Background(), func(ctx context.Context) error {
		stream, err := loop.Dial(ctx, "tcp", "example.com:80")
		if err != nil {
			return err
		}
		defer stream.Close()

		header := "GET / HTTP/1.0\r\nHost: example.com\r\nUser-Agent: curl/8.6.0\r\nAccept: */*\r\n\r\n"
		stream.Write(ctx, []byte(header))

		for chunk, err := range stream.Stream(ctx, 1024) {
			if err != nil {
				return err
			}
			fmt.Print(string(chunk))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func sumA(vs AsyncIterable[int]) (int, error) {
	var sum int
	for v, err := range vs {
		if err != nil {
			return 0, err
		}
		sum += v
	}
	return sum, nil
}

func slowCounter(ctx context.Context, max int) AsyncIterable[int] {
	return Iter(func(yield func(int) error) error {
		for i := range max {
			if err := yield(i); err != nil {
				return err
			}
			if err := Sleep(ctx, time.Second); err != nil {
				return err
			}
		}
		return nil
	})
}

func TestIter(t *testing.T) {
	err := NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
		task1 := SpawnTask(ctx, func(ctx context.Context) (any, error) {
			for v, err := range slowCounter(ctx, 10) {
				if err != nil {
					return nil, err
				}
				fmt.Println("task 1:", v)
			}
			return nil, nil
		})

		task2 := SpawnTask(ctx, func(ctx context.Context) (any, error) {
			return nil, slowCounter(ctx, 7).ForEach(func(v int) error {
				fmt.Println("task 2:", v)
				return nil
			})
		})

		task3 := SpawnTask(ctx, func(ctx context.Context) (int, error) {
			return sumA(slowCounter(ctx, 5))
		})

		var sum int
		Wait(WaitFirstError, task1, task2, task3.WriteResultTo(&sum)).MustAwait(ctx)
		fmt.Println("sum:", sum)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
