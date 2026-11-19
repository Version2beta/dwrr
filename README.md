# dwrr

`github.com/version2beta/dwrr` is a Go library for fair scheduling across queues, with per-item costs. It implements deficit weighted round robin (DWRR) over FIFO queues of any element type.

<!-- site:skip -->
Also published at [version2beta.com](https://version2beta.com/tools/dwrr-go/).
<!-- /site:skip -->

## The need

When several producers share one consumer, a single busy producer can take most of the consumer's attention:
- A first-in, first-out queue serves bursts in arrival order, so one producer's backlog delays everyone else.
- Strict priority starves the lower levels.
- Plain round robin counts turns rather than work, so a producer with expensive items gets more than its share.

Deficit round robin keeps one queue per producer and serves them in turn. Each queue earns credit in proportion to its weight and spends it on the costs you declare for each item: bytes, tokens, estimated work, or simply 1 per item. Over time each busy queue gets its weighted share of the total cost served, and an idle queue gives its share to the others.

## At a glance

| | |
|---|---|
| Latest published release | v0.10.1 (previous API) |
| This checkout | Unreleased API changes; no release version selected |
| Go | 1.22.2 or newer |
| License | MIT |
| Install published release | `go get github.com/version2beta/dwrr@v0.10.1` |
| Repository | [github.com/version2beta/dwrr](https://github.com/version2beta/dwrr) |
| API reference | [pkg.go.dev/github.com/version2beta/dwrr](https://pkg.go.dev/github.com/version2beta/dwrr) |

This README and its examples describe the **unreleased working-tree API**, not
v0.10.1. Installing v0.10.1 does not provide callable quanta or the two-result
`Take` below; see the [v0.10.1 documentation](https://github.com/version2beta/dwrr/tree/v0.10.1)
for that release. To try these examples before publication, use this checkout
(for an external consumer, a local `go.mod` `replace` pointing here).

### Migrating from v0.10.1

- Replace `batch := s.Take(n)` with `batch, err := s.Take(n)` and handle `err`
  before processing the batch. Errors return a nil batch, with no partial
  scheduling commit. Fixed-only busy queues still return nil errors.
- `SetQuantum(queue, uint64Value)` calls still work, but its parameter is now
  `any`; update interfaces or stored method signatures that require `uint64`.
  Convert separately defined numeric types explicitly to a supported builtin.
- To opt in to per-grant policies, pass an exact `func() uint64`, such as
  `stream.GetQuantum`, not its result. Handle invalid-result errors and
  `ErrStateChanged`; decide whether retries suit your application. Policies may
  run speculatively and concurrently, outside scheduler locks. Synchronize their
  state; do not rely on side effects being rolled back or on constant-path speed.
- Item costs remain fixed at enqueue; callable quanta change allocation, not cost.

## Quick start

Two queues, with quanta of 2 and 3. Queue 1's first item costs 4, more than one quantum, so it waits a round to accumulate enough credit.

```go
package main

import (
	"fmt"

	"github.com/version2beta/dwrr"
)

func main() {
	s := dwrr.NewDWRR[string](2)
	if err := s.SetQuantum(0, 2); err != nil {
		panic(err)
	}
	if err := s.SetQuantum(1, 3); err != nil {
		panic(err)
	}
	if err := s.Enqueue(0, []dwrr.Item[string]{{Value: "a", Cost: 2}, {Value: "b", Cost: 2}}); err != nil {
		panic(err)
	}
	if err := s.Enqueue(1, []dwrr.Item[string]{{Value: "x", Cost: 4}, {Value: "y", Cost: 1}}); err != nil {
		panic(err)
	}

	batch, err := s.Take(10)
	if err != nil {
		panic(err)
	}
	for _, item := range batch {
		fmt.Printf("queue %d: %s (cost %d)\n", item.Queue, item.Value, item.Cost)
	}
}
```

Output:

```text
queue 0: a (cost 2)
queue 0: b (cost 2)
queue 1: x (cost 4)
queue 1: y (cost 1)
```

## Examples

### Sharing bandwidth by bytes

Three tenants share one sender. Each item's cost is its size in bytes, and tenant `bravo` is weighted to get twice the bandwidth of the others.

```go
package main

import (
	"fmt"

	"github.com/version2beta/dwrr"
)

func main() {
	tenants := []string{"alpha", "bravo", "charlie"}
	s := dwrr.NewDWRR[string](uint(len(tenants)))
	for q, quantum := range []uint64{1500, 3000, 1500} {
		if err := s.SetQuantum(uint(q), quantum); err != nil {
			panic(err)
		}
	}

	// Every tenant has a large backlog of mixed-size messages.
	sizes := []uint64{200, 1400, 900, 64, 1500, 700}
	for q, name := range tenants {
		var items []dwrr.Item[string]
		for i := 0; i < 200; i++ {
			size := sizes[(i+q)%len(sizes)]
			items = append(items, dwrr.Item[string]{Value: fmt.Sprintf("%s-%d", name, i), Cost: size})
		}
		if err := s.Enqueue(uint(q), items); err != nil {
			panic(err)
		}
	}

	// Send 300 messages and count bytes per tenant.
	sent := make([]uint64, len(tenants))
	batch, err := s.Take(300)
	if err != nil {
		panic(err)
	}
	for _, item := range batch {
		sent[item.Queue] += item.Cost
	}
	var total uint64
	for _, b := range sent {
		total += b
	}
	for q, name := range tenants {
		fmt.Printf("%-8s %7d bytes  %4.1f%%\n", name, sent[q], 100*float64(sent[q])/float64(total))
	}
}
```

Output:

```text
alpha      59732 bytes  25.2%
bravo     119100 bytes  50.3%
charlie    58132 bytes  24.5%
```

The shares follow the weights in bytes, even though message sizes vary from 64 to 1,500 bytes.

### A worker loop

`Take` never waits for work to arrive: when the scheduler is empty, it returns an empty slice. Like every other method, it may wait briefly for the scheduler's lock. Producers enqueue work and send a non-blocking wake-up; the worker takes a batch at a time and waits on the wake channel when the scheduler is empty.

```go
package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/version2beta/dwrr"
)

type job struct {
	tenant int
	id     int
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	s := dwrr.NewDWRR[job](3)
	wake := make(chan struct{}, 1)
	signal := func() {
		select {
		case wake <- struct{}{}:
		default: // A wake-up is already pending.
		}
	}

	var producers sync.WaitGroup
	for tenant := 0; tenant < 3; tenant++ {
		producers.Add(1)
		go func(tenant int) {
			defer producers.Done()
			for id := 0; id < 1000; id++ {
				if err := s.Enqueue(uint(tenant), []dwrr.Item[job]{{Value: job{tenant, id}, Cost: 1}}); err != nil {
					panic(err)
				}
				signal()
			}
		}(tenant)
	}

	done := make(chan []int)
	go func() {
		handled := make([]int, 3)
		for {
			batch, err := s.Take(32)
			if err != nil {
				panic(err)
			}
			if len(batch) == 0 {
				select {
				case <-wake:
					continue
				case <-ctx.Done():
					// Take anything enqueued between the last check and cancellation.
					remaining, err := s.Take(1 << 20)
					if err != nil {
						panic(err)
					}
					for _, item := range remaining {
						handled[item.Value.tenant]++
					}
					done <- handled
					return
				}
			}
			for _, item := range batch {
				handled[item.Value.tenant]++ // Do the work here.
			}
		}
	}()

	producers.Wait()
	cancel()
	fmt.Println("jobs handled per tenant:", <-done)
}
```

Output:

```text
jobs handled per tenant: [1000 1000 1000]
```

### Application-defined per-queue quanta

`SetQuantum(queue, source)` accepts a numeric constant or an exact `func() uint64`.
An application queue can own a `GetQuantum` method and supply its method value:

```go
if err := s.SetQuantum(queue, stream.GetQuantum); err != nil {
    panic(err)
}
batch, err := s.Take(32)
// Handle err before processing batch; no partial batch is returned on error.
```

The scheduler calls that queue's function **at every new grant**, not just once
per `Take`, and not when resuming a visit that already received credit. The
function may return a constant, but the scheduler cannot assume it will. `Take(n)`
still batches retrieval: even `Take(1)` may need many grants to afford a head.
Empty queues are not queried. Calls are speculative: a failed `Take` can discard
planned grants, but not policy side effects. Stop delivering by letting a queue
become empty, not by returning zero (which is an error, not a disabled queue).

[The executable deadline example](example_test.go) gives each synthetic stream a
`GetQuantum` method. The application supplies nonnegative elapsed `time.Duration`
timestamps and absolute deadlines from the **same monotonic origin**, with no
wall-clock reading or actual passage of time. Its policy returns the minimum
quantum outside a positive ramp interval, interpolates with exact integer
arithmetic (rounded down) as the deadline approaches, and clamps to the maximum
at or past the deadline. It rejects negative timestamps, nonpositive ramps, and
invalid quantum bounds. Wide multiplication avoids duration/quantum overflow.
This policy lives entirely outside the library; no clocks, callbacks under locks,
background updates, or media-specific behavior are built into the scheduler.

**Allocation is not a deadline guarantee.** Grants are algorithmic visits, not
elapsed-time ticks; execution time and completion deadlines remain application
responsibilities. Synchronize any state captured by a function. Functions run
outside the scheduler lock and can run concurrently in concurrent `Take` calls.

**Cost of callable policies:** while any busy queue has a function, `Take` cannot
skip unproductive rounds: later grants may return different values. Huge costs
with tiny callable quanta can therefore require an impractical number of calls,
including repeated calls to other busy queues. Use numeric constants when the
allocation is fixed to retain fast-forward. A batch that starts with a busy
function uses the unskipped planner for that entire batch, even if that queue
empties before the batch finishes. Callable scheduling also copies
queue metadata and up to `min(n, queue length)` costs per queue for optimistic
planning; it does not copy payloads or hold the lock while evaluating policy.

### Withdrawing work

`DrainQueue` discards everything waiting in a queue, for example when the client behind it disconnects. `RemoveHead` withdraws a single item from the front.

```go
package main

import (
	"fmt"

	"github.com/version2beta/dwrr"
)

func main() {
	s := dwrr.NewDWRR[string](2)
	if err := s.Enqueue(0, []dwrr.Item[string]{{Value: "report-1", Cost: 1}, {Value: "report-2", Cost: 1}}); err != nil {
		panic(err)
	}
	if err := s.Enqueue(1, []dwrr.Item[string]{{Value: "export-1", Cost: 1}, {Value: "export-2", Cost: 1}, {Value: "export-3", Cost: 1}}); err != nil {
		panic(err)
	}

	for _, item := range s.DrainQueue(1) {
		fmt.Println("discarded", item.Value)
	}
	if item, ok := s.RemoveHead(0); ok {
		fmt.Println("withdrew", item.Value)
	}
	batch, err := s.Take(10)
	if err != nil {
		panic(err)
	}
	for _, item := range batch {
		fmt.Println("ran", item.Value)
	}
}
```

Output:

```text
discarded export-1
discarded export-2
discarded export-3
withdrew report-1
ran report-2
```

## Behavior worth knowing

### Queues and items

- `NewDWRR[T](count)` creates `count` queues, each with quantum 1. The zero value is unsupported; use the constructor.
- Queue indices start at 0 and stay stable while a queue exists.
  - `AddQueue()` appends an empty queue at quantum 1 and returns its index.
  - `RemoveQueue()` removes only the last queue, and a later `AddQueue` may reuse its index.
  - Indices aren't permanent IDs, so they suit fixed or slowly changing sets of queues.
- `Item[T]` has a `Value` and a positive `Cost`.
  - `Enqueue` copies each item record, so changing the caller's slice later can't change an accepted cost.
  - Values of `T` are copied shallowly.
- `Scheduled[T]` reports the source queue, value and cost of each item `Take` returns.
- Invalid queue indices panic, as slice indexing does.

### Scheduling

- **Visits.** The scheduler visits queues in index order, cycling. At the start of a visit it adds the queue's quantum to the queue's credit, then serves FIFO heads while the credit covers their cost, subtracting each cost.
- **Blocking within a queue.** A head the queue can't afford ends the visit and blocks the items behind it.
- **Carry-over.** Unused credit carries forward while a queue has items, which is how an item costing more than one quantum eventually becomes eligible. An empty queue's credit resets to zero.
- **Idle rounds.** With numeric constants on every busy queue, the scheduler skips whole unproductive rounds in one step, adding each waiting queue's own quantum per skipped round, then resumes visits in order. A function on any busy queue disables skipping so every new grant can evaluate its current policy.
- **Fairness is over declared costs,** not execution time, bytes on the wire or wall-clock use, unless those are what your costs measure.

### `Take`

- `Take(n)` returns `([]Scheduled[T], error)`. On success it returns up to `n` items in service order, fewer only when queued work runs out. **The error return is an intentional API change:** existing callers must handle it.
- **The limit is a retrieval limit, not a fairness cap.** If it interrupts a visit, the visit and its remaining credit persist, and the next call continues without granting another quantum.
- `Take(0)` returns an empty, non-nil slice and nil error without changing anything or evaluating any functions. A negative `n` panics first.
- With no queues or no work, `Take` returns an empty, non-nil slice and nil error.
- The returned slice never shares storage with the scheduler's queues.
- Invalid callback results return `ErrZeroQuantum` or `ErrQuantumTooLarge`, with a nil batch. Callback panics propagate. Neither commits speculative selections, credit, or cursor changes, even if the failure follows earlier planned selections.
- Concurrent mutation during callable scheduling returns a nil batch and `ErrStateChanged`, not an automatic callback retry. Handle this explicitly (for example, retry in the application if appropriate). Callback side effects, including scheduler mutations performed by a callback, are **not** rolled back. Other goroutines' committed work is unaffected.
- If a callback result is invalid and state also changed, the quantum error takes precedence; state is checked only after a valid plan completes.

### Limits and errors

- Costs and quanta are limited to `MaxCost` and `MaxQuantum`, both `uint64(1<<63 - 1)`.
- `Enqueue` returns `ErrZeroCost` or `ErrCostTooLarge` and changes nothing if any item in the batch is invalid.
- `SetQuantum` checks the queue index first. Its `any` source accepts builtin `int`, `int8/16/32/64`, `uint`, `uint8/16/32/64`, `uintptr`, `float32/64`, or exact `func() uint64`. Aliases such as `byte` work; separately defined numeric/function types and other signatures/types panic (convert explicitly).
- Constants must be positive whole numbers within `MaxQuantum`. Zero returns `ErrZeroQuantum`; negative, fractional, NaN/infinite values or a nil function return `ErrInvalidQuantum`; oversized whole numbers return `ErrQuantumTooLarge`. Floats are never silently rounded: `float64(MaxQuantum)` rounds to 2⁶³ and is rejected. Rejection changes nothing.
- Function results are validated on each grant during `Take`, not during `SetQuantum`; functions must return a positive `uint64` no greater than `MaxQuantum`.
- Credit uses exact `uint64` arithmetic:
  - Outside a visit, a queue's credit is at most `MaxCost − 1`.
  - A grant adds at most `MaxQuantum`.
  - So credit never exceeds `2⁶⁴ − 3`, which fits in a `uint64`. A test reaches that value.

### Changing queues during a visit

- **`Enqueue`** appends items. Items added to the queue being visited join that visit without another quantum.
- **`SetQuantum`** keeps the queue's credit and any visit in progress, including fixed-to-function and function-to-fixed changes. The new source applies at the queue's next grant, not when a paused visit resumes. Setting the same numeric constant is a no-op; setting a function replaces the source without evaluating it.
- **`RemoveHead`** removes the front item without scheduling it. It returns the item and `true`, or the zero item and `false` when the queue is empty. If items remain, the queue's credit and any visit in progress are kept. If the queue becomes empty, its credit resets and its visit ends.
- **`DrainQueue`** removes and returns every item, resets the queue's credit, and ends its visit.
- **`AddQueue`** doesn't affect existing indices or a visit in progress.
- **`RemoveQueue`** drops the last queue and its contents. Removing the queue being visited ends that visit, and the scheduler's position stays valid. Removing from no queues does nothing.
- **Storage.** A queue emptied by `Take` or `RemoveHead` keeps its remaining storage for reuse. `DrainQueue` and `RemoveQueue` release it.

### Concurrency

- Scheduler state is mutex-protected. With only constants on busy queues, `Take` selects a batch under that lock. Callable scheduling snapshots cost prefixes, queue lengths, sources, credit and position under lock, evaluates policy outside all scheduler locks, and commits only if its state generation is unchanged. This makes the whole successful batch atomic with respect to other operations, including remove/re-add of the same queue index.
- Every actual mutation invalidates outstanding plans. Two concurrent callable `Take` calls can evaluate the same source; only a plan whose generation still matches can commit. A callback can call scheduler methods without a scheduler-lock deadlock, but mutating state invalidates its outer plan. Avoid uncontrolled recursive policy calls and synchronize application state yourself.
- Generations never wrap: after 2⁶⁴−1 state changes, the next mutation panics before changing state, preventing an old snapshot from matching a reused generation. This also applies to fixed-only scheduling: current-Go conformance to the shared all-sequences specification is qualified by this lifetime restriction, not an added shared limit for Elixir or Python.
- `Take(0)` returns without locking, and a negative `Take` panics before locking. Don't copy a `DWRR` after first use.
- `Take` selects and removes work; running it, and running it concurrently, is up to you.
- The lock protects the scheduler's state, not the values you store in `T` or mutable state used by `GetQuantum`. Values of `T` are still shallow-copied.

## How we know it's correct

- **A deliberately naive reference.** The test suite includes a reference scheduler that visits one queue at a time, never skips anything, and is short enough to check by reading.
- **Differential tests** run thousands of random operation sequences against the reference and this library, including per-grant functions and changes made in the middle of a visit. Results, credit and independent callback event traces must match exactly. A fuzz target preserves the original operation decoder and also exercises functions in a second pass; CI fuzzes it on every push.
- **Large-number tests.** A second reference with arbitrary-precision credit covers costs and quanta near the 2⁶³ limit.
- **Exactness.** A boundary test drives credit to its maximum, `2⁶⁴ − 3`.

[TESTING.md](https://github.com/version2beta/dwrr/blob/main/TESTING.md) describes each test file and the reasoning behind these checks.

### Check it yourself

1. Clone the repository and run the tests:
   ```sh
   git clone https://github.com/version2beta/dwrr.git
   cd dwrr
   go test -race ./...
   ```
2. Fuzz against the reference for a minute:
   ```sh
   go test -run='^$' -fuzz=FuzzDifferential -fuzztime=60s
   ```
3. Plant a bug. In `dwrr.go`, find the check in `takeFixed` that decides whether a queue can afford its next item:
   ```go
   if d.deficits[q] < head.Cost {
   ```
   Change `<` to `<=`, then run:
   ```sh
   go test -run TestDifferentialRandom
   ```
   The test reports what the library returned, what the reference returned, and the sequence of operations that produced the difference. Try this only in a disposable copy, or save `dwrr.go` first and restore that saved copy afterward. `git checkout dwrr.go` would discard all uncommitted implementation work.

## How it compares

<!-- Re-verify these facts before each publish. -->

| | dwrr | [tadglines/wfq](https://github.com/tadglines/wfq) | [vanadium drrqueue](https://pkg.go.dev/v.io/x/ref/runtime/internal/lib/bqueue/drrqueue) |
|---|---|---|---|
| Algorithm | Deficit round robin | Weighted fair queuing | Deficit round robin with strict priority levels |
| Per-item cost | Declared `uint64` | Item size | Buffer size in bytes |
| Weights | Per queue | Per item, 1–256 | Shared quantum |
| Retrieval | Does not wait for work: `Take(n)` | Blocking, with `Close` | Blocking `Get` |
| Size limits | None | Total and per-flow | Not documented |
| Item type | Generic `T` | `interface{}` | Byte buffers |
| Status | Maintained | Last updated 2014 | Internal; can't be imported from outside its module |

Packages that implement smooth weighted round robin choose which backend receives the next request. They don't hold queued work, so they solve a different problem.

**When to choose something else:**
- If you want a blocking producer–consumer queue with size limits built in, wfq provides that; with dwrr you'd write a thin wrapper.
- If per-item latency matters more than throughput fairness, weighted fair queuing orders items by virtual finish time and generally interleaves more finely, at the cost of a heap operation per item.
- If your queues come and go constantly, such as one per short-lived tenant, dwrr's index-based queues are a poor fit. Map stable keys to indices yourself, or use a scheduler with keyed queues.

## Colophon

Design and copyright by M. Robert Martin.\
Implemented with assistance from Claude Opus 5.5 and OpenAI gpt-6.1-sol.\
License: MIT License.
