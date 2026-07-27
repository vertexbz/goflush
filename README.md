# goflush

![goflush](goflush.png)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A lightweight, type-safe reactive stream library for Go 1.27+.
Built on generics — no reflection, no external dependencies.

---

## Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Error Handling](#error-handling)
- [Full Pipeline Example](#full-pipeline-example)
- [Concurrency Model](#concurrency-model)
- [Subscription Lifecycle](#subscription-lifecycle)
- [API Reference](#api-reference)

---

## Overview

**goflush** models data as lazy, composable streams.  
Nothing executes until `Subscribe` or `Observe` is called.  
Operators compose by wiring callbacks — synchronous operators add zero goroutines; async operators (like `Debounce`) add exactly one goroutine per subscription so the concurrency budget is always explicit and predictable.

---

## Quick Start

### Emit from a slice or literal values

```go
import "github.com/vertexbz/goflush"

goflush.FromSlice([]int{1, 2, 3}).Subscribe(func(v int) {
    fmt.Println(v)
})

goflush.FromValues("a", "b", "c").Subscribe(func(v string) {
    fmt.Println(v)
})
```

### Imperative push with `NewEmitter`

```go
e := goflush.NewEmitter[string]()

e.Stream().Observe(goflush.NewObserver[string](
    func(v string) { fmt.Println("value:", v) },
    func(err error) { fmt.Println("error:", err) },
    func() { fmt.Println("done") },
))

e.Emit("hello")
e.EmitError(errors.New("oops"))
e.Emit("world")
e.Close()
```

> `Emit`, `EmitError`, and `Close` call subscriber callbacks **synchronously on the caller's goroutine**. No goroutines are added.

### Producer goroutine with `NewSource`

```go
ctx := context.Background()

src := goflush.NewSource(ctx, func(emit func(int), emitErr func(error)) {
    for i := 1; i <= 5; i++ {
        emit(i)
    }
    // optionally: emitErr(someErr)
})

done := make(chan struct{})
src.Observe(goflush.NewObserver[int](
    func(v int) { fmt.Println(v) },
    nil,
    func() { close(done) },
))
<-done
```

> `NewSource` starts **exactly 1 goroutine** when the first subscriber subscribes. The subscriber's `Next` callback runs on that producer goroutine.

---

## Error Handling

All three callbacks in `Observer` are optional — pass `nil` to silently discard that signal.

```go
src.Observe(goflush.NewObserver[int](
    func(v int) { /* handle value */ },
    func(err error) { /* handle error */ },
    func() { /* source finished cleanly */ },
))
```

### `ErrorCatch` — replace an error with a fallback stream

```go
e := goflush.NewEmitter[int]()

e.Stream().
    ErrorCatch(func(err error) *goflush.Stream[int] {
        fmt.Printf("caught: %v — using fallback\n", err)
        return goflush.FromValues(0)
    }).
    Subscribe(func(v int) { fmt.Println(v) })

e.EmitError(errors.New("boom")) // prints: caught: boom — using fallback \n 0
```

### `ErrorMap` — transform errors before they propagate

```go
e.Stream().
    ErrorMap(func(err error) error {
        return fmt.Errorf("wrapped: %w", err)
    }).
    Observe(goflush.NewObserver[int](
        nil,
        func(err error) { fmt.Println(err) },
        nil,
    ))
```

### `MapErr` — transform values with potential errors

```go
e.Stream().
    MapErr(func(v int) (string, error) {
        if v < 0 {
            return "", goflush.Drop // silently discard this event
        }
        return fmt.Sprintf("%d", v), nil
    }).
    Subscribe(func(s string) { fmt.Println(s) })
```

> Return `goflush.Drop` from a `MapErr` or `MuxErr` callback to silently discard an event without emitting a value or error.

---

## Full Pipeline Example

Two concurrent sources merged, filtered, mapped, debounced, and consumed via a channel:

```go
nums    := goflush.NewEmitter[int]()
letters := goflush.NewEmitter[int]()

// Merge emits from both sources; no goroutines added by Merge itself.
// Filter, Map: synchronous callbacks, no goroutines.
// Debounce: adds 1 goroutine (timer); subscriber called on timer goroutine.
// Channel: adds 1 goroutine (context watchdog); values delivered via buffered channel.
pipeline := goflush.Merge(nums.Stream(), letters.Stream()).
    Filter(func(v int) bool { return v%2 == 0 }).
    Map(func(v int) string { return fmt.Sprintf("even:%d", v) }).
    Debounce(20 * time.Millisecond)

ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

ch := pipeline.Channel(ctx) // goroutine budget: 2 (Debounce + Channel watchdog)

go func() {
    for i := 1; i <= 10; i++ { nums.Emit(i) }
    nums.Close()
    for i := 11; i <= 20; i++ { letters.Emit(i) }
    letters.Close()
}()

for v := range ch {
    fmt.Println(v) // range closes when ctx expires or both sources complete
}
```

### Mux — combine latest values from multiple sources

`Mux` is like `Merge` but instead of forwarding raw values it calls a user-provided function with a pointer to the **most recent value** from each source. Sources that have not yet emitted use `nil`.

```go
a := goflush.NewEmitter[int]()
b := goflush.NewEmitter[int]()

goflush.Mux(func(values ...*int) string {
    aStr, bStr := "nil", "nil"
    if values[0] != nil { aStr = fmt.Sprintf("%d", *values[0]) }
    if values[1] != nil { bStr = fmt.Sprintf("%d", *values[1]) }
    return fmt.Sprintf("a=%s b=%s", aStr, bStr)
}, a.Stream(), b.Stream()).Subscribe(func(s string) {
    fmt.Println(s)
})

a.Emit(1)  // → "a=1 b=nil"
b.Emit(2)  // → "a=1 b=2"
a.Emit(3)  // → "a=3 b=2"
```

> No goroutines are added by `Mux` itself. A mutex guards the shared last-value state so concurrent sources are safe.

### Series — subscribe to sources sequentially

`Series` subscribes to each source in order; the next source starts only after the previous one completes.

```go
first  := goflush.FromValues(1, 2, 3)
second := goflush.FromValues(4, 5, 6)

goflush.Series(first, second).Subscribe(func(v int) {
    fmt.Println(v) // prints 1 2 3 4 5 6 in order
})
```

---

## Concurrency Model

### Goroutine budget per subscription

| Source / Operator | Goroutines added |
|---|---|
| `FromSlice`, `FromValues`, `FromGenerator`, `FromGeneratorErr` | 0 — subscriber called synchronously on subscribe call stack |
| `NewEmitter` (`Emit` / `EmitError` / `Close`) | 0 — subscriber called on whichever goroutine calls Emit |
| `NewSource` | **+1** — dedicated producer goroutine; subscriber called on it |
| `Filter`, `Distinct`, `Take`, `Skip` | 0 |
| `Map`, `FlatMap`, `MapErr`, `Scan` | 0 |
| `Buffer` | 0 |
| `Series` | 0 — subscribes to sources one at a time; relies on each source's goroutines |
| `Merge` | 0 — relies on source goroutines; adds a mutex for safe fan-in |
| `Mux`, `MuxErr` | 0 — relies on source goroutines; adds a mutex for safe fan-in |
| `ErrorCatch`, `ErrorMap`, `ErrorFilter`, `ErrorDistinct`, `ErrorSkip` | 0 |
| `Debounce` | **+1** — uses `time.AfterFunc`; subscriber called on the timer goroutine |
| `Throttle` | 0 — uses wall-clock comparison; subscriber called on the upstream goroutine |
| `Channel`, `ErrorChannel` | **+1** — context-watchdog goroutine closes the channel on cancellation |
| `Into`, `ErrorInto` | 0 — blocks the caller's goroutine |

### Subscriber call context

| Operator | Subscriber runs on... |
|---|---|
| `FromSlice` / `FromValues` | Subscribe call stack |
| `NewEmitter` | Goroutine that calls `Emit` / `EmitError` / `Close` |
| `NewSource` | Producer goroutine |
| `Debounce` | Timer goroutine (`time.AfterFunc`) |
| `Throttle` | Upstream goroutine (first event of each window) |
| `Channel` / `ErrorChannel` | Values sent to a buffered channel; consumer goroutine reads them |
| All other operators | Same goroutine as upstream |

---

## Subscription Lifecycle

Every `Subscribe` / `Observe` call returns a `Subscription`:

```go
type Subscription interface {
    Close()
}
```

- Call `Close()` to cancel at any time.
- After `Close()`, no further `Next` or `Error` callbacks are delivered.
- Goroutines and timers owned by that subscription are stopped.
- `Complete` is called **at most once** — when the source ends naturally, before any `Close()` the caller might issue.

---

## API Reference

### Core types

| | Description |
|---|---|
| `Observer[T]` | Interface with `Next(T)`, `Error(error)`, `Complete()` methods |
| `NewObserver[T](next, err, complete)` | Creates an `Observer[T]` from optional callbacks; nil callbacks are no-ops |
| `Subscription` | Interface with a single `Close()` method |
| `Drop` | Sentinel error; return from `MapErr`/`MuxErr` to silently discard an event |

### Sources

| | Description |
|---|---|
| `FromSlice[T](items []T) *Stream[T]` | Emits slice elements synchronously; calls `Complete` after last item |
| `FromValues[T](values ...T) *Stream[T]` | Variadic convenience wrapper over `FromSlice` |
| `FromGenerator[T](gen iter.Seq[T]) *Stream[T]` | Emits from a Go iterator synchronously |
| `FromGeneratorErr[T](gen iter.Seq2[T, error]) *Stream[T]` | Iterator variant; non-nil errors forwarded to `Observer.Error` |
| `NewEmitter[T]() *Emitter[T]` | Push-based subject; call `Emit`, `EmitError`, `Close` imperatively |
| `NewSource[T](ctx, produce) *Stream[T]` | Producer-function source; starts `produce` in one goroutine on first subscribe |

### Operators — same type (methods on `*Stream[T]`)

| | Description |
|---|---|
| `.Filter(pred) *Stream[T]` | Forward only values satisfying `pred` |
| `.Distinct() *Stream[T]` | Suppress consecutive duplicate values |
| `.Take(n) *Stream[T]` | Forward at most `n` values, then complete |
| `.Skip(n) *Stream[T]` | Drop the first `n` values |
| `.Debounce(d) *Stream[T]` | Emit a value only after `d` silence; **+1 goroutine** |
| `.Throttle(d) *Stream[T]` | Forward first value of each `d`-width window; no goroutines added |
| `.ErrorCatch(fn) *Stream[T]` | Replace an error with a fallback stream |
| `.ErrorMap(fn) *Stream[T]` | Transform errors before they propagate |
| `.ErrorFilter(pred) *Stream[T]` | Forward only errors satisfying `pred`; drop the rest |
| `.ErrorDistinct() *Stream[T]` | Suppress consecutive duplicate errors (compared with `errors.Is`) |
| `.ErrorSkip(n) *Stream[T]` | Drop the first `n` errors, then forward the rest |

### Operators — type-changing (methods, Go 1.27 generic method type parameters)

| | Description |
|---|---|
| `.Map[B](fn) *Stream[B]` | Transform each value to type `B` |
| `.FlatMap[B](fn) *Stream[B]` | Map each value to a `*Stream[B]` and flatten |
| `.MapErr[B](fn) *Stream[B]` | Map each value to `(B, error)`; return `Drop` to silently discard |
| `.Scan[B](seed, fn) *Stream[B]` | Running accumulator; emits state after each value |

### Free-function operators

| | Description |
|---|---|
| `Buffer[T](src, n) *Stream[[]T]` | Batch values into slices of size `n`; partial batch flushed on `Complete` |
| `Merge[T](sources...) *Stream[T]` | Fan-in: forward values from all sources as they arrive |
| `Series[T](sources...) *Stream[T]` | Sequential: subscribe to each source in order, one after the previous completes |
| `Mux[T,R](fn, sources...) *Stream[R]` | Call `fn(...*T)` with last-known pointer per source on every event |
| `MuxErr[T,R](fn, sources...) *Stream[R]` | Like `Mux` but `fn` may return an error; return `Drop` to discard an event |

### Consumption

| | Description |
|---|---|
| `.Subscribe(fn) Subscription` | Subscribe with `Next` callback only |
| `.SubscribeErr(fn) Subscription` | Subscribe with a combined `func(T, error)` callback |
| `.Observe(obs) Subscription` | Subscribe with a full `Observer[T]` (Next, Error, Complete) |
| `.Channel(ctx) <-chan T` | Convert to a Go channel; **+1 goroutine**; ⚠ errors silently dropped |
| `.ErrorChannel(ctx) <-chan error` | Convert the error stream to a Go channel; **+1 goroutine**; ⚠ values silently dropped |
| `.Into(ctx, ch chan<- T)` | Push into a caller-provided channel; blocks caller; ⚠ errors silently dropped |
| `.ErrorInto(ctx, ch chan<- error)` | Push errors into a caller-provided channel; blocks caller; ⚠ values silently dropped |

> **`Channel`, `ErrorChannel`, `Into`, and `ErrorInto` silently drop signals not matching their type.** Use `Observe` directly when full error handling is required.
