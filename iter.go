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

type Iterator[V any] iter.Seq[V]

func (i Iterator[V]) Collect() []V {
	var vs []V
	for v := range i {
		vs = append(vs, v)
	}
	return vs
}

type MapIterator[K comparable, V any] iter.Seq2[K, V]

func (mi MapIterator[K, V]) Collect() map[K]V {
	m := make(map[K]V)
	for k, v := range mi {
		m[k] = v
	}
	return m
}

func AsSeq[V any, VS []V](slice VS) Iterator[V] {
	return func(yield func(V) bool) {
		for i := range slice {
			if !yield(slice[i]) {
				return
			}
		}
	}
}

func AsSeq2[K comparable, V any](m map[K]V) MapIterator[K, V] {
	return func(yield func(K, V) bool) {
		for k, v := range m {
			if !yield(k, v) {
				return
			}
		}
	}
}

func Range(count int) Iterator[int] {
	return func(yield func(int) bool) {
		for i := 0; i < count; i++ {
			if !yield(i) {
				return
			}
		}
	}
}

func Count(start int) Iterator[int] {
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

func ZipLongest[T, U any, TI Iterable[T], UI Iterable[U]](it1 TI, it2 UI) iter.Seq2[T, U] {
	return func(yield func(T, U) bool) {
		next1, stop1 := iter.Pull(iter.Seq[T](it1))
		next2, stop2 := iter.Pull(iter.Seq[U](it2))
		defer stop1()
		defer stop2()

		for {
			v1, ok1 := next1()
			v2, ok2 := next2()
			if (!ok1 && !ok2) || !yield(v1, v2) {
				return
			}
		}
	}
}

func Enumerate[T any, TS Iterable[T]](start int, it TS) iter.Seq2[int, T] {
	return Zip(Count(start), it)
}

func Map[T, U any, TS Iterable[T]](it TS, f func(T) U) Iterator[U] {
	return func(yield func(U) bool) {
		for t := range it {
			if !yield(f(t)) {
				return
			}
		}
	}
}

func FlatMap[T, U any, TS Iterable[T], US Iterable[U]](it TS, f func(T) US) Iterator[U] {
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

func Filter[T, TS Iterable[T]](it TS, f func(T) bool) Iterator[T] {
	return func(yield func(T) bool) {
		for t := range it {
			if !f(t) || !yield(t) {
				return
			}
		}
	}
}

func Uniq[V comparable, VS Iterable[V]](it VS) Iterator[V] {
	return func(yield func(V) bool) {
		m := make(map[V]struct{})
		for v := range it {
			if _, ok := m[v]; !ok {
				if !yield(v) {
					return
				}
				m[v] = struct{}{}
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

func (ai AsyncIterable[T]) UntilErr(err *error) Iterator[T] {
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
		var earlyStop bool
		if err := f(func(val T) error {
			if !yield(val, nil) {
				earlyStop = true
				return context.Canceled
			}
			return nil
		}); err != nil && !earlyStop {
			var zero T
			yield(zero, err)
		}
	}
}
