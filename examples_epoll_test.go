//go:build linux && !channels

package asyncigo_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/arvidfm/asyncigo"
)

// This shows a more advanced usage of AsyncIter, combining yields and awaits.
// It reads lines of data from an asynchronous stream and yields the length of each line.
func ExampleAsyncIter_dial() {
	defer serveFile("stream_example2.txt", "6172")()

	if err := asyncigo.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
		it := asyncigo.AsyncIter(func(yield func(int) error) error {
			stream, err := asyncigo.RunningLoop(ctx).Dial(ctx, "tcp", "localhost:6172")
			if err != nil {
				return err
			}

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
}

// UntilErr is particularly useful for ranging over network streams.
func ExampleAsyncIterable_UntilErr_asyncStreams_A() {
	defer serveFile("stream_example1.txt", "6172")()

	if err := asyncigo.NewEventLoop().Run(context.Background(), func(ctx context.Context) error {
		stream, _ := asyncigo.RunningLoop(ctx).Dial(ctx, "tcp", "localhost:6172")

		var err error
		for line := range stream.Lines(ctx).UntilErr(&err) {
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
}

// Since UntilErr returns a standard single-valued iterator,
// you can easily manipulate the iterator using functions like [asyncigo.Map]
// and [asyncigo.Enumerate].
// This example shows how you could combine multiple asynchronous iterators
// using [asyncigo.Chain].
func ExampleAsyncIterable_UntilErr_asyncStreams_B() {
	defer serveFile("stream_example1.txt", "6172")()
	defer serveFile("stream_example2.txt", "6173")()

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
}

func serveFile(path, port string) (close func()) {
	data, err := os.ReadFile(filepath.Join("tests", path))
	if err != nil {
		panic(err)
	}

	address := net.JoinHostPort("localhost", port)
	var waiter sync.WaitGroup
	var l net.Listener
	waiter.Add(1)
	go func() {
		defer waiter.Done()

		var err error
		l, err = net.Listen("tcp", address)
		if err != nil {
			panic(err)
		}

		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}

			if _, err := conn.Write(data); err != nil {
				panic(err)
			}
			_ = conn.Close()
		}
	}()

	for {
		conn, err := net.Dial("tcp", address)
		if errors.Is(err, syscall.ECONNREFUSED) {
			time.Sleep(time.Millisecond * 10)
		} else if err != nil {
			panic(err)
		} else {
			_ = conn.Close()
			break
		}
	}

	return func() {
		_ = l.Close()
		waiter.Wait()
	}
}
