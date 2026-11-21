# Changelog

## v0.11.0

Unstable pre-1 release; the shared fixed-quantum specification remains a draft.

### API and behavior

- **Breaking:** `Take(n)` now returns `([]Scheduled[T], error)`, rather than a
  single batch. Callers must handle the error before processing items; interfaces
  and stored method signatures must be updated. See the README migration notes.
- `SetQuantum(queue, source any)` accepts builtin numeric constants or an exact
  `func() uint64` (including method values). Defined numeric/function types require
  explicit conversion; unsupported types panic. Invalid constants and nil
  functions return errors without changing state.
- Callable sources are evaluated at every new credit grant, possibly many times
  per `Take`, but not on empty queues or when resuming a credited visit. Credit
  survives source changes; costs remain fixed when enqueued.
- Invalid callback results return `ErrZeroQuantum` or `ErrQuantumTooLarge`;
  concurrent state changes return `ErrStateChanged`. Errors return a nil batch,
  and callback panics propagate, without committing speculative scheduling state.
  Invalid-result errors take precedence over concurrent state changes.
- Policies run outside scheduler locks and may be invoked speculatively and
  concurrently. Side effects are not rolled back and callbacks are not retried
  automatically; applications own synchronization and retry decisions.
- Numeric-only busy queues retain fast-forward. A busy callable source disables
  skipping for the batch and adds optimistic metadata/cost-prefix allocations;
  huge costs with tiny callable quanta can require impractically many grants.
- Add dynamic differential, boundary, rollback and concurrency coverage, an
  application-owned deadline-policy example and oracle tests, and a callable
  scheduling benchmark. Allocation is not a deadline guarantee.

### Documentation and internals

- README rewritten as the library's documentation and tool page; TESTING.md added.
- README concurrency wording corrected: `Take` never waits for work, but may wait for the scheduler's lock.
- Internal: visit closing shared; hot `Take` removal bookkeeping kept local to avoid helper-call overhead.
- Benchmarks: `BenchmarkTakeDense16` renamed `BenchmarkTakeRotating16`; `BenchmarkTakeBacklogged16` added.

## v0.10.1

- Module path moved to `github.com/version2beta/dwrr`. No code changes.
- v0.9.0 and v0.10.0 remain available at `github.com/EnvelopeInc/dwrr`.

## v0.10.0

- Replace the unit-cost `Do` API with cost-aware `Take`, returning items in scheduler service order. Retrieval limits preserve a paused visit and its credit.
- Keep deficit and any active visit when `SetQuantum` changes a queue's weight. The new quantum applies at the next grant, preventing repeated reweighting from starving large-cost heads.
- Limit costs and quanta to `MaxCost` and `MaxQuantum`, both `uint64(1<<63 - 1)`. Oversized inputs return `ErrCostTooLarge` or `ErrQuantumTooLarge` without changing state. Exact fixed-width deficits replace `math/big`.
- Skip empty queues with a busy-queue bitmap and bound result allocation by the queued item count, preserving service order while improving sparse-queue performance.
- Reuse remaining queue storage when `Take` or `RemoveHead` empties a queue; `DrainQueue` and removing a queue release its storage.
- Add an independent naive reference model, deterministic mutation comparisons, boundary tests, differential fuzz testing, and allocation-reporting benchmarks. Arbitrary-precision top-of-range differential tests and a peak-credit reweighting test verify exact deficit arithmetic. Stable-Go CI runs the fuzz target.
