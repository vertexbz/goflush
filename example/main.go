package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vertexbz/goflush"
)

func main() {
	fmt.Println("=== goflush demo ===")

	demoSubject()
	demoProducerSource()
	demoFilterDistinctTakeSkip()
	demoTypeChangingOps()
	demoBuffering()
	demoAsyncOps()
	demoErrorHandling()
	demoFullPipeline()

	fmt.Println("\n=== done ===")
}

// demoSubject shows the imperative push model: create a Subject, subscribe with a full
// Observer (Next + Error + Complete), then drive it manually.
// No goroutines are added — every callback fires on the caller's goroutine.
func demoSubject() {
	fmt.Println("\n--- Subject (imperative push) ---")

	e := goflush.NewEmitter[string]()

	// SubscribeObserver wires up all three callbacks at once.
	e.Stream().Observe(goflush.NewObserver[string](
		func(v string) { fmt.Printf("  next: %q\n", v) },
		func(err error) { fmt.Printf("  error: %v\n", err) },
		func() { fmt.Println("  complete") },
	))

	e.Emit("hello")
	e.EmitError(errors.New("something went wrong"))
	e.Emit("world") // emitted after an error — errors do not terminate the stream
	e.Close()       // triggers the Complete callback and removes all subscribers
}

// demoProducerSource shows NewSource: the producer function runs in its own goroutine
// (1 goroutine added per subscription). Complete is called after the producer returns.
func demoProducerSource() {
	fmt.Println("\n--- NewSource (producer goroutine) ---")

	ctx := context.Background()
	src := goflush.NewSource(ctx, func(emit func(int), emitErr func(error)) {
		for i := 1; i <= 4; i++ {
			emit(i)
		}
		emitErr(errors.New("producer finished with error"))
		// Complete is called automatically after this function returns.
	})

	done := make(chan struct{})
	src.Observe(goflush.NewObserver[int](
		func(v int) { fmt.Printf("  produced: %d\n", v) },
		func(err error) { fmt.Printf("  producer error: %v\n", err) },
		func() { close(done) },
	))
	<-done // wait for the producer goroutine to finish
}

// demoFilterDistinctTakeSkip shows the four purely synchronous filtering operators.
// None of them add goroutines; the subscriber is called on the same goroutine as upstream.
func demoFilterDistinctTakeSkip() {
	fmt.Println("\n--- Filter / Distinct / Take / Skip ---")

	nums := goflush.FromValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	// Filter: only even numbers pass through.
	fmt.Print("  Filter(even):   ")
	nums.Filter(func(v int) bool { return v%2 == 0 }).
		Subscribe(func(v int) { fmt.Printf("%d ", v) })
	fmt.Println()

	// Distinct: suppress consecutive duplicate values.
	fmt.Print("  Distinct:       ")
	goflush.FromValues(1, 1, 2, 3, 3, 3, 2, 2).Distinct().
		Subscribe(func(v int) { fmt.Printf("%d ", v) })
	fmt.Println()

	// Take: emit at most n values, then signal Complete and stop.
	fmt.Print("  Take(4):        ")
	nums.Take(4).Subscribe(func(v int) { fmt.Printf("%d ", v) })
	fmt.Println()

	// Skip: ignore the first n values, then forward the rest.
	fmt.Print("  Skip(7):        ")
	nums.Skip(7).Subscribe(func(v int) { fmt.Printf("%d ", v) })
	fmt.Println()

	// Operators chain: filter evens from the first 8 values.
	fmt.Print("  Take(8)+Filter: ")
	nums.Take(8).Filter(func(v int) bool { return v%2 == 0 }).
		Subscribe(func(v int) { fmt.Printf("%d ", v) })
	fmt.Println()
}

// demoTypeChangingOps covers Map, FlatMap, and Scan — operators that change the type
// parameter and are therefore free functions rather than methods.
// No goroutines are added; subscriber is called on the upstream goroutine.
func demoTypeChangingOps() {
	fmt.Println("\n--- Map / FlatMap / Scan ---")

	// Map: transform int → string.
	fmt.Print("  Map(int→string):  ")
	goflush.FromValues(1, 2, 3).
		Map(func(v int) string {
			return fmt.Sprintf("#%d", v)
		}).
		Subscribe(func(v string) { fmt.Printf("%s ", v) })
	fmt.Println()

	// FlatMap: each value expands into a mini-source.
	fmt.Print("  FlatMap(×2 items): ")
	goflush.FromValues(1, 2, 3).
		FlatMap[string](func(v int) *goflush.Stream[string] {
		return goflush.FromValues(fmt.Sprintf("%d-a", v), fmt.Sprintf("%d-b", v))
	}).
		Subscribe(func(v string) { fmt.Printf("%s ", v) })
	fmt.Println()

	// Scan: running sum — emits the accumulator after each value.
	fmt.Print("  Scan(sum):         ")
	goflush.FromValues(1, 2, 3, 4, 5).
		Scan(0, func(acc, v int) int {
			return acc + v
		}).
		Subscribe(func(v int) { fmt.Printf("%d ", v) })
	fmt.Println()
}

// demoBuffering shows Buffer: values are accumulated into fixed-size slices.
// The last (partial) slice is flushed on Complete. No goroutines added.
func demoBuffering() {
	fmt.Println("\n--- Buffer ---")

	// Buffer(3): emits [1 2 3], [4 5 6], then flushes [7] on Complete.
	fmt.Print("  Buffer(3) of 7: ")
	goflush.Buffer[int](goflush.FromValues(1, 2, 3, 4, 5, 6, 7), 3).
		Subscribe(func(batch []int) { fmt.Printf("%v ", batch) })
	fmt.Println()
}

// demoAsyncOps shows Debounce and Throttle — the two async boundary operators.
// Each adds 0 goroutines itself but uses time.AfterFunc timers.
// Debounce: subscriber is called on the timer goroutine.
// Throttle: subscriber is called on the upstream goroutine (for the first event of each window).
func demoAsyncOps() {
	fmt.Println("\n--- Debounce / Throttle ---")

	e := goflush.NewEmitter[int]()

	// Debounce: only emits after 40 ms of silence.
	// Rapid burst {1,2,3} → only 3 passes; then 4 after a pause.
	var debounced []int
	done := make(chan struct{})
	e.Stream().Debounce(40 * time.Millisecond).Observe(goflush.NewObserver[int](
		func(v int) { debounced = append(debounced, v) },
		nil,
		func() { close(done) },
	))

	e.Emit(1)
	e.Emit(2)
	e.Emit(3)
	time.Sleep(80 * time.Millisecond) // let debounce fire
	e.Emit(4)
	time.Sleep(80 * time.Millisecond)
	e.Close()
	<-done
	fmt.Printf("  Debounce(40ms) burst{1,2,3} pause 4: %v\n", debounced)

	// Throttle: passes the first value of each 50 ms window; suppresses the rest.
	e2 := goflush.NewEmitter[int]()
	var throttled []int
	done2 := make(chan struct{})
	e2.Stream().Throttle(50 * time.Millisecond).Observe(goflush.NewObserver[int](
		func(v int) { throttled = append(throttled, v) },
		nil,
		func() { close(done2) },
	))

	e2.Emit(10) // passes (opens window)
	e2.Emit(20) // suppressed
	e2.Emit(30) // suppressed
	time.Sleep(70 * time.Millisecond)
	e2.Emit(40) // passes (new window)
	e2.Close()
	<-done2
	fmt.Printf("  Throttle(50ms) {10,20,30} pause 40: %v\n", throttled)
}

// demoErrorHandling shows MapError (transform error values) and CatchError (replace errors
// with a fallback source). Both are synchronous; no goroutines added.
func demoErrorHandling() {
	fmt.Println("\n--- MapError / CatchError ---")

	// MapError wraps every error with additional context.
	e1 := goflush.NewEmitter[int]()
	e1.Stream().
		ErrorMap(func(err error) error { return fmt.Errorf("wrapped: %w", err) }).
		Observe(goflush.NewObserver[int](
			func(v int) { fmt.Printf("  MapError value: %d\n", v) },
			func(err error) { fmt.Printf("  MapError error: %v\n", err) },
			nil,
		))
	e1.Emit(1)
	e1.EmitError(errors.New("original"))
	e1.Close()

	// CatchError intercepts an error and subscribes to a fallback source instead.
	e2 := goflush.NewEmitter[int]()
	e2.Stream().
		ErrorCatch(func(err error) *goflush.Stream[int] {
			fmt.Printf("  CatchError caught %q → fallback\n", err)
			return goflush.FromValues(-1, -2)
		}).
		Subscribe(func(v int) { fmt.Printf("  CatchError value: %d\n", v) })
	e2.Emit(42)
	e2.EmitError(errors.New("boom"))
	e2.Close()
}

// demoFullPipeline assembles a realistic end-to-end pipeline:
//
//	two NewSubject sources → Merge → Filter → Map → Debounce → Channel → range
//
// Goroutine budget: 1 (context watchdog from Channel).
// The subscriber (range loop) runs on the current goroutine consuming from the channel.
func demoFullPipeline() {
	fmt.Println("\n--- Full pipeline: Merge → Filter → Map → Debounce → Channel ---")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	nums := goflush.NewEmitter[int]()
	more := goflush.NewEmitter[int]()

	// Build the pipeline using method chaining where the type stays the same,
	// and Map (type int→string) as a free function.
	pipeline := goflush.Merge[int](nums.Stream(), more.Stream()).
		Filter(func(v int) bool { return v%2 == 0 }).
		Map(func(v int) string { return fmt.Sprintf("even:%d", v) }).
		Debounce(25 * time.Millisecond)

	// Channel wraps Subscribe and returns a <-chan string.
	// Errors from upstream are silently dropped by Channel; use SubscribeObserver
	// directly if you need error handling on the channel path.
	ch := pipeline.Channel(ctx)

	// Emit from a separate goroutine to simulate concurrent producers.
	go func() {
		for i := 1; i <= 6; i++ {
			nums.Emit(i)
		}
		nums.Close()

		for i := 7; i <= 12; i++ {
			more.Emit(i)
		}
		more.Close()
	}()

	count := 0
	for v := range ch {
		fmt.Printf("  got: %s\n", v)
		count++
	}
	fmt.Printf("  total items received: %d\n", count)
}
