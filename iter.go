package asyncio_go

import (
	"context"
	"iter"
)

type Iterable[V any] interface {
	~func(func(V) bool)
}

type Iterable2[K, V any] interface {
	~func(func(K, V) bool)
}

type MapIterable[K comparable, V any] interface {
	~func(func(V) bool) | ~map[K]V
}

type IntIterable[V any] interface {
	MapIterable[int, V] | []V
}

func AsSeq[V any, VS []V](slice VS) iter.Seq[V] {
	return func(yield func(V) bool) {
		for i := range slice {
			if !yield(slice[i]) {
				return
			}
		}
	}
}

func AsSeq2[K comparable, V any](m map[K]V) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for k, v := range m {
			if !yield(k, v) {
				return
			}
		}
	}
}

func Range(count int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := 0; i < count; i++ {
			if !yield(i) {
				return
			}
		}
	}
}

func Count(start int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := start; ; i++ {
			if !yield(i) {
				return
			}
		}
	}
}

func Zip[T, U any, TI Iterable[T], UI Iterable[U]](it1 TI, it2 UI) iter.Seq2[T, U] {
	return func(yield func(T, U) bool) {
		next1, stop1 := iter.Pull(iter.Seq[T](it1))
		next2, stop2 := iter.Pull(iter.Seq[U](it2))
		defer stop1()
		defer stop2()

		for {
			v1, ok1 := next1()
			v2, ok2 := next2()
			if !ok1 || !ok2 || !yield(v1, v2) {
				return
			}
		}
	}
}

func Enumerate[T any, TS Iterable[T]](start int, it TS) iter.Seq2[int, T] {
	return Zip(Count(start), it)
}

func Map[T, U any, TS Iterable[T]](it TS, f func(T) U) iter.Seq[U] {
	return func(yield func(U) bool) {
		for t := range it {
			if !yield(f(t)) {
				return
			}
		}
	}
}

func FlatMap[T, U any, TS Iterable[T], US Iterable[U]](it TS, f func(T) US) iter.Seq[U] {
	return func(yield func(U) bool) {
		for t := range it {
			for u := range f(t) {
				if !yield(u) {
					return
				}
			}
		}
	}
}

func Filter[T, TS Iterable[T]](it TS, f func(T) bool) iter.Seq[T] {
	return func(yield func(T) bool) {
		for t := range it {
			if !f(t) || !yield(t) {
				return
			}
		}
	}
}

type AsyncIterable[T any] iter.Seq2[T, error]

func (ai AsyncIterable[T]) ForEach(f func(T) error) error {
	for v, err := range ai {
		if err != nil {
			return err
		}
		if err := f(v); err != nil {
			return err
		}
	}
	return nil
}

func (ai AsyncIterable[T]) UntilErr(err *error) iter.Seq[T] {
	return func(yield func(T) bool) {
		for v, thisErr := range ai {
			if thisErr != nil {
				*err = thisErr
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}

func AsyncIter[T any](f func(yield func(T) error) error) AsyncIterable[T] {
	return func(yield func(T, error) bool) {
		if err := f(func(val T) error {
			if !yield(val, nil) {
				return context.Canceled
			}
			return nil
		}); err != nil {
			var zero T
			yield(zero, err)
		}
	}
}
