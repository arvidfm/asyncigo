package asyncigo

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"syscall"
)

// AsyncStream is a byte stream that can be read from and written to asynchronously.
type AsyncStream struct {
	file AsyncReadWriteCloser

	buffer []byte

	writeLock Mutex
}

// NewAsyncStream constructs a new [AsyncStream].
func NewAsyncStream(file AsyncReadWriteCloser) *AsyncStream {
	return &AsyncStream{
		file: file,
	}
}

// Close closes the stream.
func (a *AsyncStream) Close() error {
	return a.file.Close()
}

func (a *AsyncStream) read(ctx context.Context, maxBytes int) (n int, err error) {
	if len(a.buffer) >= maxBytes {
		return maxBytes, nil
	}

	if cap(a.buffer) < maxBytes {
		a.buffer = slices.Grow(a.buffer, maxBytes)
	}

	for {
		readN, err := a.file.Read(a.buffer[len(a.buffer):maxBytes])
		if readN > 0 {
			a.buffer = a.buffer[:len(a.buffer)+readN]
		}

		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			if err = a.file.WaitForReady(ctx); err == nil {
				continue
			}
		}

		return len(a.buffer), err
	}
}

// Write writes the given data to the stream.
// The returned [Awaitable] can be awaited to be sure that all data has been written before continuing.
func (a *AsyncStream) Write(ctx context.Context, data []byte) Awaitable[int] {
	return SpawnTask(ctx, func(ctx context.Context) (int, error) {
		// prevent chunks from being interleaved if multiple tasks are writing at the same time
		if err := a.writeLock.Lock(ctx); err != nil {
			return 0, err
		}
		defer a.writeLock.Unlock()

		var bytesWritten int
		for {
			n, err := a.file.Write(data)
			if n > 0 {
				bytesWritten += n
				data = data[n:]
			}

			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				err = a.file.WaitForReady(ctx)
			}
			if err != nil || len(data) == 0 {
				return bytesWritten, err
			}
		}
	})
}

func (a *AsyncStream) consumeInto(buf []byte) (n int) {
	n = copy(buf, a.buffer)
	copy(a.buffer, a.buffer[n:])
	a.buffer = a.buffer[:len(a.buffer)-n]
	return n
}

func (a *AsyncStream) consume(maxBytes int) []byte {
	buf := make([]byte, min(maxBytes, len(a.buffer)))
	n := a.consumeInto(buf)
	return buf[:n]
}

func (a *AsyncStream) consumeAll() []byte {
	buf := slices.Clone(a.buffer)
	a.buffer = a.buffer[:0]
	return buf
}

// Stream returns an AsyncIterable that yields the next chunk of data as soon as it is available.
// The chunks will be no larger than the given buffer size.
func (a *AsyncStream) Stream(ctx context.Context, bufSize int) AsyncIterable[[]byte] {
	return AsyncIter(func(yield func([]byte) error) error {
		for {
			n, err := a.read(ctx, bufSize)
			if n > 0 {
				if err := yield(a.consumeAll()); err != nil {
					return err
				}
			}
			if errors.Is(err, io.EOF) {
				return nil
			} else if err != nil {
				return err
			}
		}
	})
}

// Chunks returns an AsyncIterable that iterates over the stream in fixed-size chunks of data.
func (a *AsyncStream) Chunks(ctx context.Context, chunkSize int) AsyncIterable[[]byte] {
	return AsyncIter(func(yield func([]byte) error) error {
		for {
			var err error
			for len(a.buffer) < chunkSize && err == nil {
				_, err = a.read(ctx, chunkSize)
			}
			if len(a.buffer) > 0 {
				if err := yield(a.consume(chunkSize)); err != nil {
					return err
				}
			}
			if errors.Is(err, io.EOF) {
				return nil
			} else if err != nil {
				return err
			}
		}
	})
}

func (a *AsyncStream) yieldLines(yield func([]byte) error, data []byte) error {
	start := 0
	for i, b := range data {
		if b == '\n' || i == len(data)-1 {
			if err := yield(data[start : i+1]); err != nil {
				return err
			}
			start = i + 1
		}
	}
	return nil
}

// Lines returns an AsyncIterable that iterates over all lines in the stream.
// The newline character will be included with each line.
func (a *AsyncStream) Lines(ctx context.Context) AsyncIterable[[]byte] {
	return AsyncIter(func(yield func([]byte) error) error {
		bufSize := 1024
		scanned := 0
		for {
			_, err := a.read(ctx, bufSize)
			if errors.Is(err, io.EOF) {
				return a.yieldLines(yield, a.consumeAll())
			} else if err != nil {
				return err
			}

			for i := len(a.buffer) - 1; i >= scanned; i-- {
				if a.buffer[i] == '\n' {
					if err := a.yieldLines(yield, a.consume(i+1)); err != nil {
						return err
					}
					break
				}
			}
			scanned = len(a.buffer)
			if len(a.buffer) >= bufSize {
				bufSize *= 2
			}
		}
	})
}

// ReadLine returns all data until a newline is encountered, including the newline.
func (a *AsyncStream) ReadLine(ctx context.Context) ([]byte, error) {
	return a.ReadUntil(ctx, '\n')
}

// ReadUntil returns all data until the given character is encountered, including the character.
func (a *AsyncStream) ReadUntil(ctx context.Context, character byte) ([]byte, error) {
	for i, b := range a.buffer {
		if b == character {
			return a.consume(i + 1), nil
		}
	}

	bufSize := 1024
	for {
		n, err := a.read(ctx, bufSize)
		for i := len(a.buffer) - n; i < len(a.buffer); i++ {
			if a.buffer[i] == character {
				return a.consume(i + 1), nil
			}
		}
		if errors.Is(err, io.EOF) && len(a.buffer) > 0 {
			return a.consumeAll(), nil
		} else if err != nil {
			return nil, err
		}

		if len(a.buffer) >= bufSize {
			bufSize *= 2
		}
	}
}

// ReadChunk reads a single fixed-size chunk of data from the stream.
func (a *AsyncStream) ReadChunk(ctx context.Context, chunkSize int) ([]byte, error) {
	var err error
	for len(a.buffer) < chunkSize && err == nil {
		_, err = a.read(ctx, chunkSize)
	}
	if err == nil || errors.Is(err, io.EOF) && len(a.buffer) > 0 {
		return a.consume(chunkSize), nil
	}
	return nil, err
}

// ReadAll reads until the end of the stream and returns all read data.
func (a *AsyncStream) ReadAll(ctx context.Context) ([]byte, error) {
	var buf bytes.Buffer
	var err error
	for chunk := range a.Stream(ctx, 1024).UntilErr(&err) {
		buf.Write(chunk)
	}
	return buf.Bytes(), err
}
