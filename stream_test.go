package goflush_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vertexbz/goflush"
)

// ---- helpers ----

func collect[T any](src *goflush.Stream[T]) []T {
	var result []T
	src.Subscribe(func(v T) { result = append(result, v) })
	return result
}

func collectWithError[T any](src *goflush.Stream[T]) ([]T, error) {
	var result []T
	var gotErr error
	src.Observe(goflush.NewObserver[T](
		func(v T) { result = append(result, v) },
		func(err error) { gotErr = err },
		nil,
	))
	return result, gotErr
}

// ---- FromSlice / Of ----

func TestFromSlice(t *testing.T) {
	got := collect(goflush.FromSlice([]int{1, 2, 3}))
	assert.Equal(t, []int{1, 2, 3}, got)
}

func TestFromSliceComplete(t *testing.T) {
	completed := false
	goflush.FromSlice([]int{1, 2}).Observe(goflush.NewObserver[int](
		func(v int) {},
		nil,
		func() { completed = true },
	))
	assert.True(t, completed, "expected Complete to be called")
}

func TestOf(t *testing.T) {
	got := collect(goflush.FromValues(10, 20, 30))
	assert.Equal(t, []int{10, 20, 30}, got)
}

func ExampleFromValues() {
	got := collect(goflush.FromValues(1, 2, 3))
	fmt.Println(got)
	// Output: [1 2 3]
}

func ExampleFromSlice() {
	items := []string{"a", "b", "c"}
	got := collect(goflush.FromSlice(items))
	fmt.Println(got)
	// Output: [a b c]
}

// ---- FromGenerator / FromGeneratorErr ----

func TestFromGenerator(t *testing.T) {
	gen := func(yield func(int) bool) {
		for _, v := range []int{1, 2, 3} {
			if !yield(v) {
				return
			}
		}
	}
	got := collect(goflush.FromGenerator(gen))
	assert.Equal(t, []int{1, 2, 3}, got)
}

func TestFromGeneratorComplete(t *testing.T) {
	gen := func(yield func(int) bool) {
		yield(10)
		yield(20)
	}
	completed := false
	goflush.FromGenerator(gen).Observe(goflush.NewObserver[int](
		func(int) {},
		nil,
		func() { completed = true },
	))
	assert.True(t, completed, "expected Complete to be called")
}

func ExampleFromGenerator() {
	gen := func(yield func(int) bool) {
		for i := 1; i <= 3; i++ {
			if !yield(i) {
				return
			}
		}
	}
	got := collect(goflush.FromGenerator(gen))
	fmt.Println(got)
	// Output: [1 2 3]
}

func TestFromGeneratorErr(t *testing.T) {
	sentinel := errors.New("gen error")
	gen := func(yield func(int, error) bool) {
		yield(1, nil)
		yield(0, sentinel)
		yield(2, nil)
	}
	var got []int
	var gotErr error
	goflush.FromGeneratorErr(gen).Observe(goflush.NewObserver[int](
		func(v int) { got = append(got, v) },
		func(err error) { gotErr = err },
		nil,
	))
	assert.Equal(t, []int{1, 2}, got)
	assert.ErrorIs(t, gotErr, sentinel)
}

func TestFromGeneratorErrComplete(t *testing.T) {
	gen := func(yield func(int, error) bool) {
		yield(1, nil)
		yield(2, nil)
	}
	completed := false
	goflush.FromGeneratorErr(gen).Observe(goflush.NewObserver[int](
		func(int) {},
		nil,
		func() { completed = true },
	))
	assert.True(t, completed, "expected Complete to be called")
}

func TestFromGeneratorErrNilErrorCallback(t *testing.T) {
	sentinel := errors.New("dropped")
	gen := func(yield func(int, error) bool) {
		yield(0, sentinel)
	}
	// must not panic when Observer.Error is nil
	assert.NotPanics(t, func() {
		goflush.FromGeneratorErr(gen).Subscribe(func(int) {})
	})
}

func ExampleFromGeneratorErr() {
	gen := func(yield func(string, error) bool) {
		yield("a", nil)
		yield("b", nil)
		yield("", errors.New("oops"))
		yield("c", nil)
	}
	var got []string
	var gotErr error
	goflush.FromGeneratorErr(gen).Observe(goflush.NewObserver[string](
		func(v string) { got = append(got, v) },
		func(err error) { gotErr = err },
		nil,
	))
	fmt.Println(got, gotErr)
	// Output: [a b c] oops
}

// ---- NewSubject / Emitter ----

func TestNewSubject(t *testing.T) {
	e := goflush.NewEmitter[int]()
	var got []int
	e.Stream().Subscribe(func(v int) { got = append(got, v) })
	e.Emit(1)
	e.Emit(2)
	e.Close()
	assert.Equal(t, []int{1, 2}, got)
}

func TestEmitError(t *testing.T) {
	e := goflush.NewEmitter[int]()
	var gotErr error
	e.Stream().Observe(goflush.NewObserver[int](
		func(v int) {},
		func(err error) { gotErr = err },
		nil,
	))
	sentinel := errors.New("test error")
	e.EmitError(sentinel)
	assert.ErrorIs(t, gotErr, sentinel)
}

func TestEmitAfterClose(t *testing.T) {
	e := goflush.NewEmitter[int]()
	count := 0
	e.Stream().Subscribe(func(v int) { count++ })
	e.Close()
	e.Emit(1) // must be no-op
	assert.Equal(t, 0, count, "expected no events after Close")
}

func TestEmitErrorAfterClose(t *testing.T) {
	e := goflush.NewEmitter[int]()
	var gotErr error
	e.Stream().Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.Close()
	e.EmitError(errors.New("should be ignored")) // must be no-op
	assert.NoError(t, gotErr, "expected no error after Close")
}

func TestNilErrorCallback(t *testing.T) {
	// Must not panic when Error callback is nil
	e := goflush.NewEmitter[int]()
	e.Stream().Subscribe(func(int) {}) // no Error callback
	assert.NotPanics(t, func() {
		e.EmitError(errors.New("dropped"))
	})
}

func ExampleNewEmitter() {
	e := goflush.NewEmitter[string]()
	var got []string
	e.Stream().Subscribe(func(v string) { got = append(got, v) })
	e.Emit("hello")
	e.Emit("world")
	e.Close()
	fmt.Println(got)
	// Output: [hello world]
}

// ---- NewSource ----

func TestNewSource(t *testing.T) {
	ctx := context.Background()
	src := goflush.NewSource(ctx, func(emit func(int), _ func(error)) {
		emit(1)
		emit(2)
		emit(3)
	})
	var got []int
	done := make(chan struct{})
	src.Observe(goflush.NewObserver[int](
		func(v int) { got = append(got, v) },
		nil,
		func() { close(done) },
	))
	<-done
	assert.Equal(t, []int{1, 2, 3}, got)
}

func TestNewSourceEmitError(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("producer error")
	src := goflush.NewSource(ctx, func(_ func(int), emitErr func(error)) {
		emitErr(sentinel)
	})
	var gotErr error
	done := make(chan struct{})
	src.Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		func() { close(done) },
	))
	<-done
	assert.ErrorIs(t, gotErr, sentinel)
}

func TestNewSourceContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	src := goflush.NewSource(ctx, func(emit func(int), _ func(error)) {
		close(started)
		time.Sleep(10 * time.Second) // would block forever without cancel
	})
	src.Subscribe(func(int) {})
	<-started
	cancel()
	// test passes if it doesn't hang
}

func ExampleNewSource() {
	ctx := context.Background()
	src := goflush.NewSource(ctx, func(emit func(int), _ func(error)) {
		for i := 1; i <= 3; i++ {
			emit(i)
		}
	})
	done := make(chan struct{})
	var got []int
	src.Observe(goflush.NewObserver[int](
		func(v int) { got = append(got, v) },
		nil,
		func() { close(done) },
	))
	<-done
	fmt.Println(got)
	// Output: [1 2 3]
}

// ---- Filter ----

func TestFilter(t *testing.T) {
	got := collect(goflush.FromValues(1, 2, 3, 4, 5).Filter(func(v int) bool { return v%2 == 0 }))
	assert.Equal(t, []int{2, 4}, got)
}

func TestFilterErrorPropagation(t *testing.T) {
	e := goflush.NewEmitter[int]()
	sentinel := errors.New("filter error")
	var gotErr error
	e.Stream().Filter(func(int) bool { return true }).Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.EmitError(sentinel)
	assert.ErrorIs(t, gotErr, sentinel, "Filter: error not propagated")
}

func ExampleStream_Filter() {
	got := collect(goflush.FromValues(1, 2, 3, 4, 5).Filter(func(v int) bool { return v%2 == 0 }))
	fmt.Println(got)
	// Output: [2 4]
}

// ---- Distinct ----

func TestDistinct(t *testing.T) {
	got := collect(goflush.FromValues(1, 1, 2, 2, 3, 1).Distinct())
	assert.Equal(t, []int{1, 2, 3, 1}, got)
}

func TestDistinctErrorPropagation(t *testing.T) {
	e := goflush.NewEmitter[int]()
	sentinel := errors.New("distinct error")
	var gotErr error
	e.Stream().Distinct().Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.EmitError(sentinel)
	assert.ErrorIs(t, gotErr, sentinel, "Distinct: error not propagated")
}

// ---- Take ----

func TestTake(t *testing.T) {
	got := collect(goflush.FromValues(1, 2, 3, 4, 5).Take(3))
	assert.Equal(t, []int{1, 2, 3}, got)
}

func TestTakeComplete(t *testing.T) {
	completed := false
	goflush.FromValues(1, 2, 3, 4, 5).Take(2).Observe(goflush.NewObserver[int](
		func(int) {},
		nil,
		func() { completed = true },
	))
	assert.True(t, completed, "Take: expected Complete after n values")
}

func TestTakeErrorPropagation(t *testing.T) {
	e := goflush.NewEmitter[int]()
	sentinel := errors.New("take error")
	var gotErr error
	e.Stream().Take(10).Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.EmitError(sentinel)
	assert.ErrorIs(t, gotErr, sentinel, "Take: error not propagated")
}

// ---- Skip ----

func TestSkip(t *testing.T) {
	got := collect(goflush.FromValues(1, 2, 3, 4, 5).Skip(2))
	assert.Equal(t, []int{3, 4, 5}, got)
}

func TestSkipErrorPropagation(t *testing.T) {
	e := goflush.NewEmitter[int]()
	sentinel := errors.New("skip error")
	var gotErr error
	e.Stream().Skip(5).Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.EmitError(sentinel)
	assert.ErrorIs(t, gotErr, sentinel, "Skip: error not propagated")
}

// ---- Buffer ----

func TestBuffer(t *testing.T) {
	got := collect(goflush.Buffer(goflush.FromValues(1, 2, 3, 4, 5), 2))
	// expect [[1,2],[3,4],[5]]
	require.Len(t, got, 3, "Buffer: expected 3 slices")
	assert.Equal(t, []int{1, 2}, got[0])
	assert.Equal(t, []int{3, 4}, got[1])
	assert.Equal(t, []int{5}, got[2], "Buffer[2] (partial flush)")
}

func TestBufferN1(t *testing.T) {
	got := collect(goflush.Buffer(goflush.FromValues(1, 2, 3), 1))
	assert.Len(t, got, 3)
}

func TestBufferErrorPropagation(t *testing.T) {
	e := goflush.NewEmitter[int]()
	sentinel := errors.New("buffer error")
	var gotErr error
	goflush.Buffer[int](e.Stream(), 10).Observe(goflush.NewObserver[[]int](
		func([]int) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.EmitError(sentinel)
	assert.ErrorIs(t, gotErr, sentinel, "Buffer: error not propagated")
}

// ---- Series ----

func TestSeries(t *testing.T) {
	a := goflush.FromValues(1, 2, 3)
	b := goflush.FromValues(4, 5, 6)
	got := collect(goflush.Series(a, b))
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6}, got)
}

func TestSeriesZeroSources(t *testing.T) {
	completed := false
	goflush.Series[int]().Observe(goflush.NewObserver[int](
		func(int) {},
		nil,
		func() { completed = true },
	))
	assert.True(t, completed, "Series(0 sources): expected Complete")
}

func TestSeriesSingleSource(t *testing.T) {
	got := collect(goflush.Series(goflush.FromValues(1, 2, 3)))
	assert.Equal(t, []int{1, 2, 3}, got)
}

func TestSeriesComplete(t *testing.T) {
	a := goflush.FromValues(1, 2)
	b := goflush.FromValues(3, 4)
	completed := false
	goflush.Series(a, b).Observe(goflush.NewObserver[int](
		func(int) {},
		nil,
		func() { completed = true },
	))
	assert.True(t, completed, "Series: Complete should fire when all sources complete")
}

func TestSeriesSequential(t *testing.T) {
	// Verifies second source is not subscribed until first completes.
	ea := goflush.NewEmitter[int]()
	eb := goflush.NewEmitter[int]()

	var got []int
	done := make(chan struct{})
	goflush.Series(ea.Stream(), eb.Stream()).Observe(goflush.NewObserver[int](
		func(v int) { got = append(got, v) },
		nil,
		func() { close(done) },
	))

	ea.Emit(1)
	ea.Emit(2)
	ea.Close() // first completes → Series subscribes to eb
	eb.Emit(3)
	eb.Emit(4)
	eb.Close()

	<-done
	assert.Equal(t, []int{1, 2, 3, 4}, got, "Series: values must arrive in strict source order")
}

func TestSeriesErrorPropagation(t *testing.T) {
	sentinel := errors.New("series error")
	e := goflush.NewEmitter[int]()
	normal := goflush.FromValues(1, 2, 3)
	var gotErr error
	goflush.Series(normal, e.Stream()).Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.EmitError(sentinel)
	assert.ErrorIs(t, gotErr, sentinel, "Series: error not propagated")
}

func ExampleSeries() {
	a := goflush.FromValues(1, 2, 3)
	b := goflush.FromValues(4, 5, 6)
	got := collect(goflush.Series(a, b))
	fmt.Println(got)
	// Output: [1 2 3 4 5 6]
}

// ---- Merge ----

func TestMerge(t *testing.T) {
	a := goflush.FromValues(1, 2, 3)
	b := goflush.FromValues(4, 5, 6)
	got := collect[int](goflush.Merge(a, b))
	sort.Ints(got)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6}, got)
}

func TestMergeZeroSources(t *testing.T) {
	completed := false
	goflush.Merge[int]().Observe(goflush.NewObserver[int](
		func(int) {},
		nil,
		func() { completed = true },
	))
	assert.True(t, completed, "Merge(0 sources): expected Complete")
}

func TestMergeComplete(t *testing.T) {
	// Complete fires only after all sources have completed.
	a := goflush.FromValues(1, 2)
	b := goflush.FromValues(3, 4)
	completed := false
	goflush.Merge(a, b).Observe(goflush.NewObserver[int](
		func(int) {},
		nil,
		func() { completed = true },
	))
	assert.True(t, completed, "Merge: Complete should fire when all sources complete")
}

func TestMergeCompletePartial(t *testing.T) {
	// Complete must NOT fire while at least one source is still open.
	ea := goflush.NewEmitter[int]()
	eb := goflush.NewEmitter[int]()
	completed := false
	goflush.Merge(ea.Stream(), eb.Stream()).Observe(goflush.NewObserver[int](
		func(int) {},
		nil,
		func() { completed = true },
	))
	ea.Close() // only one source closed
	assert.False(t, completed, "Merge: Complete must not fire until all sources complete")
	eb.Close() // now both closed
	assert.True(t, completed, "Merge: Complete should fire once all sources complete")
}

func TestMergeErrorPropagation(t *testing.T) {
	sentinel := errors.New("merge error")
	e := goflush.NewEmitter[int]()
	normal := goflush.FromValues(1, 2, 3)
	merged := goflush.Merge[int](normal, e.Stream())
	var gotErr error
	merged.Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.EmitError(sentinel)
	assert.ErrorIs(t, gotErr, sentinel, "Merge: error not propagated")
}

func TestMergeRaceSafe(t *testing.T) {
	// Two concurrent emitters — verifies no data race under -race
	e1 := goflush.NewEmitter[int]()
	e2 := goflush.NewEmitter[int]()
	merged := goflush.Merge[int](e1.Stream(), e2.Stream())
	var mu sync.Mutex
	var got []int
	merged.Subscribe(func(v int) {
		mu.Lock()
		got = append(got, v)
		mu.Unlock()
	})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			e1.Emit(i)
		}
		e1.Close()
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			e2.Emit(i)
		}
		e2.Close()
	}()
	wg.Wait()
	assert.Len(t, got, 100, "MergeRace: expected 100 items")
}

func ExampleMerge() {
	a := goflush.FromValues(1, 3, 5)
	b := goflush.FromValues(2, 4, 6)
	got := collect[int](goflush.Merge(a, b))
	sort.Ints(got)
	fmt.Println(got)
	// Output: [1 2 3 4 5 6]
}

// ---- CatchError ----

func TestCatchError(t *testing.T) {
	sentinel := errors.New("upstream error")
	e := goflush.NewEmitter[int]()
	fallback := goflush.FromValues(99, 100)
	src := e.Stream().ErrorCatch(func(err error) *goflush.Stream[int] {
		return fallback
	})
	var got []int
	src.Subscribe(func(v int) { got = append(got, v) })
	e.EmitError(sentinel)
	assert.Equal(t, []int{99, 100}, got, "CatchError: expected fallback values")
}

func TestCatchErrorFallbackError(t *testing.T) {
	sentinel := errors.New("fallback error")
	e := goflush.NewEmitter[int]()
	fallbackEmitter := goflush.NewEmitter[int]()
	src := e.Stream().ErrorCatch(func(error) *goflush.Stream[int] {
		return fallbackEmitter.Stream()
	})
	var gotErr error
	src.Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.EmitError(errors.New("initial"))
	fallbackEmitter.EmitError(sentinel)
	assert.ErrorIs(t, gotErr, sentinel, "CatchError: fallback error not propagated")
}

func ExampleStream_ErrorCatch() {
	e := goflush.NewEmitter[int]()
	src := e.Stream().ErrorCatch(func(err error) *goflush.Stream[int] {
		return goflush.FromValues(0) // fallback value
	})
	var got []int
	src.Subscribe(func(v int) { got = append(got, v) })
	e.EmitError(errors.New("oops"))
	fmt.Println(got)
	// Output: [0]
}

// ---- MapError ----

func TestMapError(t *testing.T) {
	e := goflush.NewEmitter[int]()
	src := e.Stream().ErrorMap(func(err error) error {
		return fmt.Errorf("wrapped: %w", err)
	})
	var gotErr error
	src.Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		nil,
	))
	sentinel := errors.New("original")
	e.EmitError(sentinel)
	require.Error(t, gotErr)
	assert.ErrorIs(t, gotErr, sentinel, "MapError: expected wrapped sentinel")
	assert.Equal(t, "wrapped: original", gotErr.Error())
}

func ExampleStream_ErrorMap() {
	e := goflush.NewEmitter[int]()
	src := e.Stream().ErrorMap(func(err error) error {
		return fmt.Errorf("wrapped: %w", err)
	})
	var gotErr error
	src.Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.EmitError(errors.New("boom"))
	fmt.Println(gotErr)
	// Output: wrapped: boom
}

// ---- Map ----

func TestMap(t *testing.T) {
	got := collect(goflush.FromValues(1, 2, 3).Map(func(v int) string { return fmt.Sprintf("x%d", v) }))
	assert.Equal(t, []string{"x1", "x2", "x3"}, got)
}

func TestMapErrorPropagation(t *testing.T) {
	e := goflush.NewEmitter[int]()
	sentinel := errors.New("map error")
	src := e.Stream().Map(func(v int) string { return fmt.Sprintf("%d", v) })
	var gotErr error
	src.Observe(goflush.NewObserver[string](
		func(string) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.EmitError(sentinel)
	assert.ErrorIs(t, gotErr, sentinel, "Map: error not propagated")
}

func ExampleMap() {
	got := collect(goflush.FromValues(1, 2, 3).Map(func(v int) string { return fmt.Sprintf("item%d", v) }))
	fmt.Println(got)
	// Output: [item1 item2 item3]
}

// ---- FlatMap ----

func TestFlatMap(t *testing.T) {
	got := collect(goflush.FromValues(1, 2, 3).FlatMap(func(v int) *goflush.Stream[int] {
		return goflush.FromValues(v*10, v*10+1)
	}))
	assert.Equal(t, []int{10, 11, 20, 21, 30, 31}, got)
}

func TestFlatMapErrorPropagation(t *testing.T) {
	e := goflush.NewEmitter[int]()
	sentinel := errors.New("flatmap error")
	src := e.Stream().FlatMap(func(v int) *goflush.Stream[int] { return goflush.FromValues(v) })
	var gotErr error
	src.Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.EmitError(sentinel)
	assert.ErrorIs(t, gotErr, sentinel, "FlatMap: error not propagated")
}

func ExampleStream_FlatMap() {
	got := collect(goflush.FromValues(1, 2).FlatMap(func(v int) *goflush.Stream[string] {
		return goflush.FromValues(fmt.Sprintf("a%d", v), fmt.Sprintf("b%d", v))
	}))
	fmt.Println(got)
	// Output: [a1 b1 a2 b2]
}

// ---- Scan ----

func TestScan(t *testing.T) {
	got := collect(goflush.FromValues(1, 2, 3, 4).Scan(0, func(acc, v int) int { return acc + v }))
	assert.Equal(t, []int{1, 3, 6, 10}, got)
}

func TestScanErrorPropagation(t *testing.T) {
	e := goflush.NewEmitter[int]()
	sentinel := errors.New("scan error")
	src := e.Stream().Scan(0, func(acc, v int) int { return acc + v })
	var gotErr error
	src.Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.EmitError(sentinel)
	assert.ErrorIs(t, gotErr, sentinel, "Scan: error not propagated")
}

func ExampleScan() {
	got := collect(goflush.FromValues(1, 2, 3, 4).Scan(0, func(acc, v int) int { return acc + v }))
	fmt.Println(got)
	// Output: [1 3 6 10]
}

// ---- Debounce ----

func TestDebounce(t *testing.T) {
	e := goflush.NewEmitter[int]()
	src := e.Stream().Debounce(50 * time.Millisecond)
	var got []int
	var mu sync.Mutex
	done := make(chan struct{})
	src.Observe(goflush.NewObserver[int](
		func(v int) {
			mu.Lock()
			got = append(got, v)
			mu.Unlock()
		},
		nil,
		func() { close(done) },
	))
	// rapid fire — only last should survive
	e.Emit(1)
	e.Emit(2)
	e.Emit(3)
	time.Sleep(100 * time.Millisecond)
	e.Emit(4)
	time.Sleep(100 * time.Millisecond)
	e.Close()
	<-done
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []int{3, 4}, got, "Debounce: expected [3 4]")
}

func TestDebounceErrorForwarded(t *testing.T) {
	e := goflush.NewEmitter[int]()
	sentinel := errors.New("debounce error")
	src := e.Stream().Debounce(50 * time.Millisecond)
	var gotErr error
	errReceived := make(chan struct{})
	src.Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) {
			gotErr = err
			close(errReceived)
		},
		nil,
	))
	e.EmitError(sentinel)
	<-errReceived
	assert.ErrorIs(t, gotErr, sentinel, "Debounce: error not forwarded")
}

func TestDebounceNoGoroutineLeak(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	e := goflush.NewEmitter[int]()
	ch := e.Stream().Debounce(50 * time.Millisecond).Channel(ctx)
	e.Emit(1)
	cancel()
	// drain
	for range ch {
	}
	// test passes if -race doesn't flag anything
}

func ExampleStream_Debounce() {
	e := goflush.NewEmitter[int]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := e.Stream().Debounce(30 * time.Millisecond).Channel(ctx)
	e.Emit(1)
	e.Emit(2)
	time.Sleep(60 * time.Millisecond)
	e.Close()
	var got []int
	for v := range ch {
		got = append(got, v)
	}
	fmt.Println(got)
	// Output: [2]
}

// ---- Throttle ----

func TestThrottle(t *testing.T) {
	e := goflush.NewEmitter[int]()
	src := e.Stream().Throttle(50 * time.Millisecond)
	var got []int
	var mu sync.Mutex
	done := make(chan struct{})
	src.Observe(goflush.NewObserver[int](
		func(v int) {
			mu.Lock()
			got = append(got, v)
			mu.Unlock()
		},
		nil,
		func() { close(done) },
	))
	e.Emit(1) // passes
	e.Emit(2) // suppressed (within window)
	e.Emit(3) // suppressed (within window)
	time.Sleep(70 * time.Millisecond)
	e.Emit(4) // passes (new window)
	e.Close()
	<-done
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []int{1, 4}, got, "Throttle: expected [1 4]")
}

func TestThrottleErrorForwarded(t *testing.T) {
	e := goflush.NewEmitter[int]()
	sentinel := errors.New("throttle error")
	src := e.Stream().Throttle(50 * time.Millisecond)
	var gotErr error
	errReceived := make(chan struct{})
	src.Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) {
			gotErr = err
			close(errReceived)
		},
		nil,
	))
	e.EmitError(sentinel)
	<-errReceived
	assert.ErrorIs(t, gotErr, sentinel, "Throttle: error not forwarded")
}

// ---- Channel ----

func TestChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := goflush.FromValues(1, 2, 3).Channel(ctx)
	var got []int
	for v := range ch {
		got = append(got, v)
	}
	assert.Equal(t, []int{1, 2, 3}, got)
}

func TestChannelContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	e := goflush.NewEmitter[int]()
	ch := e.Stream().Channel(ctx)
	cancel()
	// channel should close
	select {
	case _, ok := <-ch:
		if ok {
			// drain remaining
			for range ch {
			}
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("Channel: did not close after ctx cancel")
	}
}

func TestChannelDropsErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := goflush.NewEmitter[int]()
	ch := e.Stream().Channel(ctx)
	e.EmitError(errors.New("dropped"))
	e.Emit(42)
	e.Close()
	var got []int
	for v := range ch {
		got = append(got, v)
	}
	assert.Equal(t, []int{42}, got, "Channel: expected [42]")
}

func ExampleStream_Channel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := goflush.FromValues(1, 2, 3).Channel(ctx)
	var got []int
	for v := range ch {
		got = append(got, v)
	}
	fmt.Println(got)
	// Output: [1 2 3]
}

// ---- Error propagation chain ----

func TestErrorPropagatesChain(t *testing.T) {
	// error flows through Map -> Filter -> Scan
	e := goflush.NewEmitter[int]()
	sentinel := errors.New("chain error")
	src := e.Stream().
		Map(func(v int) int { return v * 2 }).
		Filter(func(v int) bool { return v > 0 }).
		Scan(0, func(acc, v int) int { return acc + v })

	var gotErr error
	src.Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.EmitError(sentinel)
	assert.ErrorIs(t, gotErr, sentinel, "chain: error not propagated")
}

// ---- Mux ----

func TestMuxBothNilInitially(t *testing.T) {
	// On first event from either source, the other source's pointer is nil.
	e1 := goflush.NewEmitter[int]()
	e2 := goflush.NewEmitter[int]()

	type snapshot struct{ a, b *int }
	var got []snapshot

	goflush.Mux(func(values ...*int) snapshot {
		s := snapshot{}
		if values[0] != nil {
			v := *values[0]
			s.a = &v
		}
		if values[1] != nil {
			v := *values[1]
			s.b = &v
		}
		return s
	}, e1.Stream(), e2.Stream()).Subscribe(func(s snapshot) {
		got = append(got, s)
	})

	e1.Emit(1)
	e2.Emit(2)

	require.Len(t, got, 2)
	// First snapshot: a=1, b=nil (e2 hasn't emitted yet)
	assert.NotNil(t, got[0].a)
	assert.Equal(t, 1, *got[0].a)
	assert.Nil(t, got[0].b)
	// Second snapshot: a=1 (last from e1), b=2
	assert.NotNil(t, got[1].a)
	assert.Equal(t, 1, *got[1].a)
	assert.NotNil(t, got[1].b)
	assert.Equal(t, 2, *got[1].b)
}

func TestMuxLatestValues(t *testing.T) {
	// Each emission triggers fn with up-to-date last values.
	e1 := goflush.NewEmitter[int]()
	e2 := goflush.NewEmitter[int]()

	var sums []int
	goflush.Mux(func(values ...*int) int {
		sum := 0
		for _, p := range values {
			if p != nil {
				sum += *p
			}
		}
		return sum
	}, e1.Stream(), e2.Stream()).Subscribe(func(v int) {
		sums = append(sums, v)
	})

	e1.Emit(10) // fn([10, nil])  → 10
	e1.Emit(20) // fn([20, nil])  → 20
	e2.Emit(5)  // fn([20, 5])    → 25

	assert.Equal(t, []int{10, 20, 25}, sums)
}

func TestMuxZeroSources(t *testing.T) {
	completed := false
	goflush.Mux(func(values ...*int) int { return 0 }).Observe(goflush.NewObserver[int](
		func(int) {},
		nil,
		func() { completed = true },
	))
	assert.True(t, completed, "Mux(0 sources): expected Complete")
}

func TestMuxErrorPropagation(t *testing.T) {
	sentinel := errors.New("mux error")
	e := goflush.NewEmitter[int]()
	normal := goflush.FromValues(1, 2, 3)

	var gotErr error
	goflush.Mux(func(values ...*int) int {
		if values[0] != nil {
			return *values[0]
		}
		return 0
	}, normal, e.Stream()).Observe(goflush.NewObserver[int](
		func(int) {},
		func(err error) { gotErr = err },
		nil,
	))
	e.EmitError(sentinel)
	assert.ErrorIs(t, gotErr, sentinel, "Mux: error not propagated")
}

func TestMuxRaceSafe(t *testing.T) {
	// Two concurrent emitters — verifies no data race under -race.
	e1 := goflush.NewEmitter[int]()
	e2 := goflush.NewEmitter[int]()

	var mu sync.Mutex
	var got []int
	goflush.Mux(func(values ...*int) int {
		sum := 0
		for _, p := range values {
			if p != nil {
				sum += *p
			}
		}
		return sum
	}, e1.Stream(), e2.Stream()).Subscribe(func(v int) {
		mu.Lock()
		got = append(got, v)
		mu.Unlock()
	})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			e1.Emit(i)
		}
		e1.Close()
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			e2.Emit(i)
		}
		e2.Close()
	}()
	wg.Wait()
	assert.Len(t, got, 100, "MuxRace: expected 100 items")
}

func ExampleMux() {
	e1 := goflush.NewEmitter[int]()
	e2 := goflush.NewEmitter[int]()

	type pair struct{ a, b *int }
	var got []pair

	goflush.Mux(func(values ...*int) pair {
		p := pair{a: values[0], b: values[1]}
		return p
	}, e1.Stream(), e2.Stream()).Subscribe(func(p pair) {
		got = append(got, p)
	})

	e1.Emit(1)
	e2.Emit(2)

	fmt.Println(got[0].a != nil, got[0].b == nil) // first event: a set, b nil
	fmt.Println(*got[1].a, *got[1].b)             // second event: both set
	// Output:
	// true true
	// 1 2
}
