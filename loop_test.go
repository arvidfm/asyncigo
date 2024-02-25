package asyncigo

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func testEventLoop(t *testing.T, name string, wantErr bool, wantRuntime time.Duration, main func(ctx context.Context, loop *EventLoop, t *testing.T) error) {
	t.Run(name, func(t *testing.T) {
		start := time.Now()
		loop := NewEventLoop()

		ctx := context.Background()
		if wantRuntime > 0 {
			timeoutCtx, cancel := context.WithTimeout(ctx, wantRuntime+time.Millisecond*500)
			defer cancel()
			ctx = timeoutCtx
		}

		err := loop.Run(ctx, func(ctx context.Context) error {
			return main(ctx, loop, t)
		})
		if errors.Is(err, ErrNotImplemented) {
			t.Skipf("function not supported on this platform")
		}
		elapsed := time.Since(start)

		tolerance := wantRuntime.Seconds() / 20
		if wantRuntime > 0 && math.Abs(elapsed.Seconds()-wantRuntime.Seconds()) > tolerance {
			t.Errorf("expected %s, got: %s (difference: %f)", wantRuntime, elapsed, math.Abs(elapsed.Seconds()-wantRuntime.Seconds()))
		}
		if (err != nil) != wantErr {
			t.Errorf("expected error %v, got: %v", wantErr, err)
		} else if errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("deadline exceeded")
		}
	})
}

func TestSleep(t *testing.T) {
	syncSleep := func(_ context.Context, duration time.Duration) error {
		time.Sleep(duration)
		return nil
	}

	tests := []struct {
		name      string
		sleepFunc func(context.Context, time.Duration) error
		spawnFunc func(context.Context, func(context.Context) (int, error)) Awaitable[int]

		wantResult  string
		wantRuntime time.Duration
	}{
		{
			name:      "task async sleep",
			sleepFunc: Sleep,
			spawnFunc: func(ctx context.Context, f func(context.Context) (int, error)) Awaitable[int] {
				return SpawnTask(ctx, f)
			},

			wantResult:  "sleep_async.txt",
			wantRuntime: time.Millisecond * 550,
		},
		{
			name:      "task sync sleep",
			sleepFunc: syncSleep,
			spawnFunc: func(ctx context.Context, f func(context.Context) (int, error)) Awaitable[int] {
				return SpawnTask(ctx, f)
			},

			wantResult:  "sleep_sync.txt",
			wantRuntime: time.Millisecond * 2650,
		},
		{
			name:      "goroutine sync sleep",
			sleepFunc: syncSleep,
			spawnFunc: func(ctx context.Context, f func(context.Context) (int, error)) Awaitable[int] {
				return Go(ctx, f)
			},

			wantResult:  "sleep_async.txt",
			wantRuntime: time.Millisecond * 550,
		},
	}

	for _, tt := range tests {
		testEventLoop(t, tt.name, false, tt.wantRuntime, func(ctx context.Context, loop *EventLoop, t *testing.T) error {
			f, err := os.Open(filepath.Join("tests", tt.wantResult))
			if err != nil {
				return err
			}
			data, err := io.ReadAll(f)
			if err != nil {
				return err
			}

			var buf bytes.Buffer
			tasks := make([]Futurer, 5)
			results := make([]int, 5)
			for i := range tasks {
				tasks[i] = tt.spawnFunc(ctx, func(ctx context.Context) (int, error) {
					if err := tt.sleepFunc(ctx, time.Millisecond*10*time.Duration(i+1)); err != nil {
						return 0, err
					}

					for j := range 5 {
						buf.Write([]byte(fmt.Sprintf("task %d: %d\n", i, j)))
						if err := tt.sleepFunc(ctx, time.Millisecond*100); err != nil {
							return 0, err
						}
					}
					return i * 10, nil
				}).WriteResultTo(&results[i])
			}

			if _, err := Wait(WaitAll, tasks...).Await(ctx); err != nil {
				return err
			}

			if buf.String() != string(data) {
				t.Errorf("unexpected output: %s", buf.String())
			}

			for i := range results {
				wantResult := i * 10
				if results[i] != wantResult {
					t.Errorf("expected return value %d, got: %d", wantResult, results[i])
				}
			}
			return nil
		})
	}
}

func TestFuture_Result(t *testing.T) {
	fut1 := NewFuture[int]()
	_, err := fut1.Result()
	if !errors.Is(err, ErrNotReady) {
		t.Errorf("Result(): expected ErrNotReady, got: %v", err)
	}

	fut1.SetResult(10, nil)
	result, err := fut1.Result()
	if result != 10 {
		t.Errorf("Result(): expected 10, got: %d", result)
	}
	if err != nil {
		t.Errorf("Result(): expected nil error, got: %v", err)
	}

	fut1.Cancel(nil)
	fut1.SetResult(42, errors.New("oops"))

	result, err = fut1.Result()
	if result != 10 {
		t.Errorf("Result(): expected 10, got: %d", result)
	}
	if err != nil {
		t.Errorf("Result(): expected nil error, got: %v", err)
	}

	fut2 := NewFuture[int]()
	fut2.Cancel(nil)
	_, err = fut2.Result()
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Result(): expected context.Canceled, got: %v", err)
	}

	fut3 := NewFuture[int]()
	fut3.Cancel(sql.ErrNoRows)
	_, err = fut3.Result()
	if !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("Result(): expected sql.ErrNoRows, got: %v", err)
	}

	fut4 := NewFuture[int]()
	fut4.SetResult(42, sql.ErrConnDone)
	result, err = fut4.Result()
	if result != 42 {
		t.Errorf("Result(): expected 42, got: %d", result)
	}
	if !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("Result(): expected sql.ErrConnDone, got: %v", err)
	}
}

func TestGoroutineHasNoLoop(t *testing.T) {
	testEventLoop(t, "goroutine has no loop", false, -1, func(ctx context.Context, loop *EventLoop, t *testing.T) error {
		result, err := Go(ctx, func(ctx context.Context) (result int, err error) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("goroutine did not panic")
				}
				result = 42
			}()
			Sleep(ctx, time.Second)
			return 0, nil
		}).Await(ctx)

		if err != nil {
			return err
		}
		if result != 42 {
			t.Errorf("unexpected return value: %d", result)
		}
		return nil
	})
}

func newPipeStream(ctx context.Context, loop *EventLoop, data []byte, bufferSize int64, throttle time.Duration) (stream *AsyncStream, close func(), err error) {
	r, w, err := loop.Pipe()
	if err != nil {
		return nil, nil, err
	}

	writer := SpawnTask(ctx, func(ctx context.Context) (any, error) {
		defer func() {
			w.Close()
		}()
		if throttle > 0 {
			buf := bytes.NewBuffer(data)
			buffer := make([]byte, bufferSize)
			for {
				n, err := buf.Read(buffer)
				if errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					panic(err)
				}

				if _, err := w.Write(ctx, buffer[:n]).Await(ctx); err != nil {
					panic(err)
				}

				Sleep(ctx, throttle)
			}
		} else {
			if _, err := w.Write(ctx, data).Await(ctx); err != nil {
				panic(err)
			}
		}
		return nil, nil
	})

	return r, func() {
		writer.Cancel(nil)
		r.Close()
		w.Close()
	}, nil
}

func newHttpStream(ctx context.Context, loop *EventLoop, data []byte, buffer int64, throttle time.Duration) (stream *AsyncStream, close func(), err error) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		buf := bytes.NewBuffer(data)
		if throttle > 0 {
			for {
				if _, err := io.CopyN(writer, buf, buffer); errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					panic(err)
				}
				time.Sleep(throttle)
			}
		} else {
			if _, err := io.Copy(writer, buf); err != nil {
				panic(err)
			}
		}
	}))

	u, err := url.Parse(ts.URL)
	if err != nil {
		return nil, nil, err
	}

	stream, err = loop.Dial(ctx, "tcp", u.Host)
	if err != nil {
		return nil, nil, err
	}
	header := fmt.Sprintf("GET / HTTP/1.0\r\n\r\n")
	stream.Write(ctx, []byte(header))

	return stream, func() {
		ts.Close()
		stream.Close()
	}, err
}

func TestAsyncStream(t *testing.T) {
	tests := []struct {
		name     string
		file     string
		buffer   int64
		throttle time.Duration
	}{
		{
			name: "many lines",
			file: "stream_1.txt",
		},
		{
			name: "long line",
			file: "stream_2.txt",
		},
		{
			name: "empty file",
			file: "stream_3.txt",
		},
		{
			name: "only newlines",
			file: "stream_4.txt",
		},
	}

	writers := []struct {
		name string
		open func(ctx context.Context, loop *EventLoop, data []byte) (stream *AsyncStream, close func(), err error)
	}{
		{
			name: "fast pipe",
			open: func(ctx context.Context, loop *EventLoop, data []byte) (stream *AsyncStream, close func(), err error) {
				return newPipeStream(ctx, loop, data, 0, 0)
			},
		},
		{
			name: "slow pipe",
			open: func(ctx context.Context, loop *EventLoop, data []byte) (stream *AsyncStream, close func(), err error) {
				return newPipeStream(ctx, loop, data, 500, time.Millisecond*10)
			},
		},
		{
			name: "fast dial",
			open: func(ctx context.Context, loop *EventLoop, data []byte) (stream *AsyncStream, close func(), err error) {
				return newHttpStream(ctx, loop, data, 0, 0)
			},
		},
		{
			name: "slow dial",
			open: func(ctx context.Context, loop *EventLoop, data []byte) (stream *AsyncStream, close func(), err error) {
				return newHttpStream(ctx, loop, data, 500, time.Millisecond*10)
			},
		},
	}

	readers := []struct {
		name string
		read func(context.Context, io.Writer, *AsyncStream) error
	}{
		{
			name: "Lines",
			read: func(ctx context.Context, writer io.Writer, stream *AsyncStream) (err error) {
				var previousLine []byte
				for line := range stream.Lines(ctx).UntilErr(&err) {
					if previousLine != nil {
						wantPos := len(previousLine) - 1
						if got := bytes.Index(previousLine, []byte("\n")); got != wantPos {
							return fmt.Errorf("expected newline at position %d, got: %d", wantPos, got)
						}
					}

					previousLine = line
					writer.Write(line)
				}
				return err
			},
		},
		{
			name: "Chunks",
			read: func(ctx context.Context, writer io.Writer, stream *AsyncStream) (err error) {
				chunkSize := 100
				var previousChunk []byte
				for chunk := range stream.Chunks(ctx, chunkSize).UntilErr(&err) {
					if previousChunk != nil && len(previousChunk) != chunkSize {
						return fmt.Errorf("expected a chunk size of %d, got: %d", chunkSize, len(previousChunk))
					}
					previousChunk = chunk
					writer.Write(chunk)
				}
				return err
			},
		},
		{
			name: "Stream",
			read: func(ctx context.Context, writer io.Writer, stream *AsyncStream) (err error) {
				for line := range stream.Stream(ctx, 1024).UntilErr(&err) {
					writer.Write(line)
				}
				return err
			},
		},
		{
			name: "ReadAll",
			read: func(ctx context.Context, writer io.Writer, stream *AsyncStream) (err error) {
				data, err := stream.ReadAll(ctx)
				if err != nil {
					return err
				}
				_, err = writer.Write(data)
				return err
			},
		},
	}

	for _, tt := range tests {
		for _, writer := range writers {
			for _, reader := range readers {
				name := fmt.Sprintf("%s_%s_%s", tt.name, writer.name, reader.name)
				testEventLoop(t, name, false, -1, func(ctx context.Context, loop *EventLoop, t *testing.T) error {
					f, err := os.Open(filepath.Join("tests", tt.file))
					if err != nil {
						return err
					}
					defer f.Close()

					data, err := io.ReadAll(f)
					if err != nil {
						return err
					}

					stream, closeStream, err := writer.open(ctx, loop, data)
					if err != nil {
						return err
					}
					defer closeStream()

					var readBuffer bytes.Buffer
					if err := reader.read(ctx, &readBuffer, stream); err != nil {
						return err
					}

					result := readBuffer.Bytes()
					resultParts := bytes.SplitN(result, []byte("\r\n\r\n"), 2)
					result = resultParts[len(resultParts)-1]
					if result == nil {
						result = []byte{}
					}
					if !reflect.DeepEqual(data, result) {
						t.Errorf("data did not match, got:\n%s", string(result))
					}

					return nil
				})
			}
		}
	}
}

func TestAsyncIterable(t *testing.T) {
	tests := []struct {
		name     string
		sleeps   []time.Duration
		errorOn  int
		breakOn  int
		cancelOn int

		wantSum     int
		wantRuntime time.Duration
		wantErr     bool
	}{
		{
			name:     "standard",
			sleeps:   []time.Duration{1, 5, 4, 3, 2},
			errorOn:  -1,
			breakOn:  -1,
			cancelOn: -1,

			wantSum:     15,
			wantRuntime: time.Millisecond * 15 * 10,
			wantErr:     false,
		},
		{
			name:     "early break",
			sleeps:   []time.Duration{1, 5, 4, 3, 2},
			errorOn:  -1,
			breakOn:  1,
			cancelOn: -1,

			wantSum:     1,
			wantRuntime: time.Millisecond * 10,
			wantErr:     false,
		},
		{
			name:     "immediate break",
			sleeps:   []time.Duration{1, 5, 4, 3, 2},
			errorOn:  -1,
			breakOn:  0,
			cancelOn: -1,

			wantSum:     0,
			wantRuntime: 0,
			wantErr:     false,
		},
		{
			name:     "immediate error",
			sleeps:   []time.Duration{1, 5, 4, 3, 2},
			errorOn:  0,
			breakOn:  -1,
			cancelOn: -1,

			wantSum:     0,
			wantRuntime: 0,
			wantErr:     true,
		},
		{
			name:     "late error",
			sleeps:   []time.Duration{1, 5, 4, 3, 2},
			errorOn:  3,
			breakOn:  -1,
			cancelOn: -1,

			wantSum:     10,
			wantRuntime: time.Millisecond * 10 * 10,
			wantErr:     true,
		},
		{
			name:     "immediate cancel",
			sleeps:   []time.Duration{1, 5, 4, 3, 2},
			errorOn:  -1,
			breakOn:  -1,
			cancelOn: 0,

			wantSum:     0,
			wantRuntime: 0,
			wantErr:     true,
		},
		{
			name:     "final cancel",
			sleeps:   []time.Duration{1, 5, 4, 3, 2},
			errorOn:  -1,
			breakOn:  -1,
			cancelOn: 4,

			wantSum:     13,
			wantRuntime: time.Millisecond * 13 * 10,
			wantErr:     true,
		},
	}

	iterators := []struct {
		name string
		f    func(context.Context, AsyncIterable[int]) (int, error)
	}{
		{
			name: "range",
			f: func(ctx context.Context, it AsyncIterable[int]) (i int, err error) {
				for v, err := range it {
					if err != nil {
						return i, err
					}
					i += v
				}
				return i, err
			},
		},
		{
			name: "UntilErr",
			f: func(ctx context.Context, it AsyncIterable[int]) (i int, err error) {
				for v := range it.UntilErr(&err) {
					i += v
				}
				return i, err
			},
		},
		{
			name: "ForEach",
			f: func(ctx context.Context, it AsyncIterable[int]) (i int, err error) {
				return i, it.ForEach(func(v int) error {
					i += v
					return nil
				})
			},
		},
	}

	for _, tt := range tests {
		for _, iterator := range iterators {
			testEventLoop(t, fmt.Sprintf("%s_%s", tt.name, iterator.name), tt.wantErr, tt.wantRuntime, func(ctx context.Context, _ *EventLoop, t *testing.T) error {
				ctx, cancel := context.WithCancel(ctx)
				sum, err := iterator.f(ctx, AsyncIter(func(yield func(int) error) error {
					for i, sleepTime := range tt.sleeps {
						if tt.breakOn == i {
							break
						} else if tt.errorOn == i {
							return errors.New("error")
						} else if tt.cancelOn == i {
							cancel()
						}
						if err := Sleep(ctx, time.Millisecond*sleepTime*10); err != nil {
							return err
						}
						if err := yield(int(sleepTime)); err != nil {
							return err
						}
					}
					return nil
				}))

				if sum != tt.wantSum {
					t.Errorf("expected sum %d, got %d", tt.wantSum, sum)
				}

				return err
			})
		}
	}
}

func TestTask_Cancel(t *testing.T) {
	for numFuts := range 10 {
		for numTasks := 1; numTasks < 10; numTasks++ {
			for cancelOn := range numFuts + 2 {
				name := fmt.Sprintf("%d_%d_%d", numFuts, numTasks, cancelOn)
				testEventLoop(t, name, false, -1, func(ctx context.Context, loop *EventLoop, t *testing.T) error {
					counts := make([]int, numTasks)
					futs := Map(Range(numFuts), func(_ int) *Future[int] {
						return NewFuture[int]()
					}).Collect()
					tasks := Map(Range(numTasks), func(i int) *Task[any] {
						return SpawnTask(ctx, func(ctx context.Context) (any, error) {
							for _, fut := range futs {
								counts[i]++
								if _, err := fut.Await(ctx); err != nil {
									return nil, err
								}
							}
							return nil, nil
						})
					}).Collect()

					tasks[0].AddDoneCallback(func(err error) {
						for _, task := range tasks {
							task.Cancel(nil)
						}
					})

					// yield to the event loop once to give the tasks a chance to start
					if err := loop.Yield(ctx, nil); err != nil {
						return err
					}

					for i, fut := range futs {
						if i == cancelOn {
							fut.Cancel(nil)
						} else {
							fut.SetResult(i, nil)
						}
					}

					for i, task := range tasks {
						if !task.HasResult() {
							t.Errorf("expected task %d to have finished, but it did not", i+1)
						}
						wantCount := min(cancelOn+1, numFuts)
						if counts[i] != wantCount {
							t.Errorf("expected task %d to return %d, but got: %d", i+1, wantCount, counts[i])
						}
					}

					wantErr := cancelOn < len(futs)
					if err := tasks[0].Err(); (err != nil) != wantErr {
						t.Errorf("expected error %t, but got: %v", wantErr, err)
					}
					for j, task := range tasks[1:] {
						if task.Err() == nil {
							t.Errorf("expected task %d to be cancelled, but it was not", j+1)
						}
					}
					return nil
				})
			}
		}
	}
}

func TestGetFirstResult(t *testing.T) {
	tests := []struct {
		name         string
		sleeps       []int
		errors       []bool
		wantRuntime  time.Duration
		wantFinished int
		wantResult   int
		wantErr      bool
	}{
		{
			name:         "basic",
			sleeps:       []int{1, 2, 3, 4, 5},
			errors:       []bool{false, false, false, false, false},
			wantRuntime:  time.Millisecond * 100,
			wantFinished: 1,
			wantResult:   10,
			wantErr:      false,
		},
		{
			name:         "reverse",
			sleeps:       []int{5, 4, 3, 2, 1},
			errors:       []bool{false, false, false, false, false},
			wantRuntime:  time.Millisecond * 100,
			wantFinished: 1,
			wantResult:   50,
			wantErr:      false,
		},
		{
			name:         "all but one",
			sleeps:       []int{3, 3, 3, 2, 3},
			errors:       []bool{false, false, false, false, false},
			wantRuntime:  time.Millisecond * 200,
			wantFinished: 1,
			wantResult:   40,
			wantErr:      false,
		},
		{
			name:         "no sleep",
			sleeps:       []int{-1, -1, -1, -1, -1, -1},
			errors:       []bool{true, true, false, false, false, false},
			wantRuntime:  0,
			wantFinished: 3,
			wantResult:   30,
			wantErr:      false,
		},
		{
			name:         "slowest finished",
			sleeps:       []int{1, 1, 1, 1, 5, 1, 1},
			errors:       []bool{true, true, true, true, false, true, true},
			wantRuntime:  time.Millisecond * 500,
			wantFinished: 7,
			wantResult:   50,
			wantErr:      false,
		},
		{
			name:         "all errors",
			sleeps:       []int{1, 1, 1, 1, 5, 1, 1},
			errors:       []bool{true, true, true, true, true, true, true},
			wantRuntime:  time.Millisecond * 500,
			wantFinished: 7,
			wantResult:   0,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		testEventLoop(t, tt.name, tt.wantErr, tt.wantRuntime, func(ctx context.Context, loop *EventLoop, t *testing.T) error {
			coros := make([]Coroutine2[int], len(tt.sleeps))
			var finished int
			for i := range len(coros) {
				coros[i] = func(ctx context.Context) (res int, err error) {
					if tt.sleeps[i] >= 0 {
						if err := Sleep(ctx, time.Millisecond*100*time.Duration(tt.sleeps[i])); err != nil {
							return 0, err
						}
					}
					if tt.errors[i] {
						err = errors.New("oops")
					}
					finished++
					return (i + 1) * 10, err
				}
			}

			res, err := GetFirstResult(ctx, coros...)
			if _, err := loop.WaitForCallbacks().Await(ctx); err != nil {
				return err
			}

			if res != tt.wantResult {
				t.Errorf("expected the result %d, got: %d", tt.wantResult, res)
			}
			if finished != tt.wantFinished {
				t.Errorf("expected %d coroutine(s) to finish, got: %d", tt.wantFinished, finished)
			}

			return err
		})
	}
}
