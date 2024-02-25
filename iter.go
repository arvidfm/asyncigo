package asyncigo

import (
	"context"
	"golang.org/x/exp/constraints"
	"iter"
)

// Iterable represents any function that can be ranged over and which yields one value.
type Iterable[V any] interface {
	~func(func(V) bool)
}

// Iterable2 represents any function that can be ranged over and which yields two values.
type Iterable2[K, V any] interface {
	~func(func(K, V) bool)
}

// Iterator is a function that can be ranged over, yielding one value each iteration.
type Iterator[V any] iter.Seq[V]

func Iter[V any, VI Iterable[V]](it VI) Iterator[V] {
	return Iterator[V](it)
}

// Collect consumes the [Iterator] and returns the yielded values as a slice.
func (i Iterator[V]) Collect() []V {
	var vs []V
	for v := range i {
		vs = append(vs, v)
	}
	return vs
}

// MapIterator is a function that can be ranged over, yielding two values each iteration.
type MapIterator[K comparable, V any] iter.Seq2[K, V]

func MapIter[K comparable, V any, I Iterable2[K, V]](it I) MapIterator[K, V] {
	return MapIterator[K, V](it)
}

// Collect consumes the [MapIterator] and returns the yielded values as a map.
func (mi MapIterator[K, V]) Collect() map[K]V {
	m := make(map[K]V)
	for k, v := range mi {
		m[k] = v
	}
	return m
}

// AsIterator returns an [Iterator] which yields the values in the slice in turn.
func AsIterator[V any, VS []V](slice VS) Iterator[V] {
	return func(yield func(V) bool) {
		for i := range slice {
			if !yield(slice[i]) {
				return
			}
		}
	}
}

// AsIterator2 returns a [MapIterator] which yields each key-value pair in the map.
func AsIterator2[K comparable, V any](m map[K]V) MapIterator[K, V] {
	return func(yield func(K, V) bool) {
		for k, v := range m {
			if !yield(k, v) {
				return
			}
		}
	}
}

// Range returns an iterator which yields every integer from 0 up to, but not including, count.
func Range[T constraints.Integer](count T) Iterator[T] {
	return func(yield func(T) bool) {
		for i := T(0); i < count; i++ {
			if !yield(i) {
				return
			}
		}
	}
}

// Count returns an infinite iterator which yields every integer starting at start.
func Count[T constraints.Integer](start T) Iterator[T] {
	return func(yield func(T) bool) {
		for i := start; ; i++ {
			if !yield(i) {
				return
			}
		}
	}
}

// Zip returns an iterator which yields each pair of items from the given iterators in turn.
// If the iterators are of different length, Zip will stop once the end of the shortest iterator
// has been reached.
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

// ZipLongest returns an iterator which yields each pair of items from the given iterators in turn.
// If the iterators are of different length, ZipLongest will continue until the end of the longest
// iterator, yielding the empty value in place of the shorter iterable.
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

// Enumerate returns an iterator which yields each value from the given iterator
// along with its index.
func Enumerate[T constraints.Integer, U any, UI Iterable[U]](start T, it UI) MapIterator[T, U] {
	return MapIterator[T, U](Zip(Count(start), it))
}

// Map returns an iterator which yields the result of passing each value from
// the given iterator through the provided mapping function in turn.
func Map[T, U any, TS Iterable[T]](it TS, f func(T) U) Iterator[U] {
	return func(yield func(U) bool) {
		for t := range it {
			if !yield(f(t)) {
				return
			}
		}
	}
}

// FlatMap returns an iterator which yields the values from the iterators returned
// by the mapping function as a single flat stream of values.
func FlatMap[T, U any, TS Iterable[T], US Iterable[U]](it TS, f func(T) US) Iterator[U] {
	return Flatten(Map(it, f))
}

// Chain yields each value from each of the given iterators as a single flat stream.
func Chain[T any, TS Iterable[T]](its ...TS) Iterator[T] {
	return Flatten(AsIterator(its))
}

// Flatten yields each value from each of the nested iterators yielded
// by the given iterator as a single flat stream.
func Flatten[T any, TS Iterable[T], TSS Iterable[TS]](its TSS) Iterator[T] {
	return func(yield func(T) bool) {
		for it := range its {
			for t := range it {
				if !yield(t) {
					return
				}
			}
		}
	}
}

// Filter returns an iterator which yields the values from the given iterator in turn,
// skipping any values for which the given filtering function return false.
func Filter[T, TS Iterable[T]](it TS, f func(T) bool) Iterator[T] {
	return func(yield func(T) bool) {
		for t := range it {
			if !f(t) || !yield(t) {
				return
			}
		}
	}
}

// Uniq returns an iterator which yields the values from the given iterator in turn,
// skipping any already encountered values.
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

// AsyncIterable is a helper type for iterating over asynchronous streams that may error.
type AsyncIterable[T any] iter.Seq2[T, error]

// ForEach calls the given function for each value yielded by this AsyncIterable
// until the iterator finishes or returns an error.
// ForEach returns an error if either the iterator or the callback function returns an error.
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

// UntilErr returns a single-values iterator that yields each non-error value
// yielded by this AsyncIterable until the iterator finishes or returns an error.
// If the AsyncIterable returns an error, the error will be written to
// the variable referenced by the given error pointer.
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

// AsyncIter is a helper function for constructing an [AsyncIterable].
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
