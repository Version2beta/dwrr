# Testing dwrr

How the test suite shows that dwrr schedules correctly, how to run it, and how to check for yourself that the tests can catch a bug.

## Running the tests

```sh
go test -race ./...                                        # full suite, with the race detector
go test -run='^$' -fuzz=FuzzDifferential -fuzztime=60s     # fuzz against the reference
go test -run='^$' -bench=. -benchmem                       # benchmarks, with allocations
go test -cover ./...                                       # statement coverage
```

For the bounded offline release-preparation commands and remaining gates, see
[RELEASING.md](RELEASING.md); current local evidence is summarized in
[HANDOFF.md](HANDOFF.md). The declared minimum Go version is 1.22.2; passing on a
newer toolchain alone does not prove minimum-version compatibility.

The only direct test dependency is `github.com/stretchr/testify`, used by the external API tests.

CI runs on every push to `main`, on every pull request, and on version tags:
- `go test -race ./...` and `go vet ./...` on Go 1.22.x and on the current stable release;
- on stable only, a 30-second fuzz run of `FuzzDifferential`.

## The idea: trust the reference, not the code

The fixed-source scheduler has optimizations that make it fast and harder to read:
- it skips empty queues using a bitmap;
- it fast-forwards through rounds in which no queue can afford its next item;
- it keeps exact credit in fixed-width integers.

Rather than trying to prove each of these correct by inspection, the tests compare the scheduler against a **reference model**: a second scheduler written to be obviously right instead of fast. It visits one queue at a time, never skips anything, and is short enough to check by reading.

The tests feed both schedulers the same long sequences of random operations, covering every public method including changes made partway through a visit. They require identical results and scheduling state at every step. For function sources, each scheduler gets its own independent script state and callback event trace. Querying once per batch, querying an active visit again, or silently skipping grants changes that trace even if the returned items happen to match.

`Take` now returns a batch and an error. The test-only `takeN` helper unwraps that error for the existing fixed-source fixtures; it still performs ONE batched `Take(n)`, not repeated single-item calls. The original 3,000 differential sequences, 2,000 mutation scenarios and their operation interpretations remain. Another 2,000 sequences cover dynamic sources. The original model in `dwrr_test.go` / `dwrr_mutation_test.go` stays fixed-source; the independent main and big-integer references cover per-grant functions.

The method has a limit. A reference checks that the code follows the rules; it can't show that the rules are right. Before v0.10.0, `SetQuantum` reset a queue's credit whenever its quantum changed. The reference model at the time followed the same rule, so it couldn't catch that a queue whose weight changes every round might never be served. A separate check that asked that question directly found the problem, and it's now a regression test, `TestSetQuantumDoesNotStarve`.

## What each test file checks

| File | What it checks |
|---|---|
| `dwrr_reference_test.go` | The naive reference model and the main differential tests. `TestDifferentialRandom` runs thousands of seeded random operation sequences against both schedulers. `FuzzDifferential` does the same with fuzz-generated input. Its first pass retains every original byte/opcode interpretation; only a second pass recognizes literal opcode 9 as installing a scripted per-grant function. Focused `TestDynamicDifferentialScenarios` precede randomized expansion. Dynamic scenarios compare callback events as well as results, deficits, source kinds and cursor/active state. Both check internal invariants after every operation: empty queues hold no credit; the scheduler's position stays in range; no visit sits on an empty queue; the busy-queue bitmap, busy count and item total match the queues. The `TestBoundary*` tests cover the largest allowed costs and quanta, rejected inputs leaving state unchanged, large fast-forward skips, and fast-forward making no allocations. |
| `dwrr_extreme_test.go` | `TestExtremeDifferentialBigReference` runs a second reference that keeps credit with `math/big`, using fixed and per-grant varying quanta of at least 2⁶² and costs up to `MaxCost`, and compares credit, results and independent callback traces at every step. `TestBoundaryPeakCreditAfterReweight` drives a queue's credit to its maximum, `2⁶⁴ − 3`. |
| `dwrr_test.go` | Scenario and unit tests, including `TestReferenceGeneratedScenarios`, which runs the original reference-style model from before v0.10.0. The tests cover service order, credit carry-over, `Take` limits and pauses, validation and panics, mutations during a visit, sustained fairness under backlog, and concurrent use under the race detector. |
| `dwrr_mutation_test.go` | `TestReferenceGeneratedMutationScenarios`: 2,000 seeded scenarios mixing every operation, checked against the original reference model. |
| `dwrr_bitmap_test.go` | The busy-queue bitmap: searches that wrap across 64-bit words, removing and reusing the last queue, and service order with sparse busy queues. |
| `dwrr_storage_test.go` | Queue storage: an emptied queue reuses its storage without allocating, retained storage holds no references to removed values, and `DrainQueue` releases storage. |
| `dwrr_adaptive_test.go` | Explicit per-grant ordering/call counts within a batch; active-visit source changes; no starvation; empty callable queues retaining constant-only fast-forward; prefix truncation preserving credit; all numeric source forms and invalid values; rollback of speculative batches after invalid results/panics; reentrant callbacks; channel-coordinated conflicts for mutation and remove/re-add; simultaneous takes and item conservation; nonwrapping generations; peak dynamic credit. |
| `dwrr_external_test.go` | The intentionally changed public API exercised from outside the package, including constants, functions and `Take` errors. |
| `example_test.go`, `deadline_policy_test.go` | Application-owned per-stream `GetQuantum` method values and supplied monotonic timestamps. Deterministic clamp/ramp/invalid-input checks plus an independent `math/big` interpolation oracle at full quantum/duration bounds. No real clock or deadline guarantees. |
| `dwrr_bench_test.go` | Benchmarks: many idle queues with one busy (`TakeSparse10k`), one item at a time rotating across 16 queues (`TakeRotating16`), 16 continuously backlogged queues (`TakeBacklogged16`), batch retrieval (`TakeBatch64`) and large costs with small quanta (`LargeCostSmallQuantum`), which exercises fast-forward. The additional `TakeDynamicBacklogged16` benchmark shows the optimistic planning cost for callable sources with bounded costs. All report allocations. |

## Why the arithmetic is exact

Credit is stored in a `uint64`. Costs and quanta are each limited to `MaxCost = MaxQuantum = 2⁶³ − 1`. The bound:

1. **Outside a visit, a queue's credit is at most `MaxCost − 1`.**
   - A visit ends only when the queue's credit is less than its next item's cost, and that cost is at most `MaxCost`.
   - Emptying a queue resets its credit to zero.
   - `RemoveHead` can expose a cheaper item, but it never increases credit.
2. **A visit begins by adding one quantum,** at most `MaxQuantum`. So credit never exceeds `MaxCost − 1 + MaxQuantum = 2⁶⁴ − 3`, which fits in a `uint64`.
3. **Fixed-only fast-forward never overshoots.** When every busy queue has a numeric constant, it skips the largest number of whole rounds that leaves every waiting queue still unable to afford its next item, so it adds at most `cost − 1 − credit` to any queue, and neither its multiplication nor its addition can overflow.

`TestBoundaryPeakCreditAfterReweight` reaches the maximum:
- a queue blocks with `MaxCost − 1` credit;
- its quantum is changed to `MaxQuantum`, keeping that credit;
- `RemoveHead` exposes a cheap item;
- the next grant brings credit to `2⁶⁴ − 3`.

`TestExtremeDifferentialBigReference` then checks that credit near the top of the range matches an arbitrary-precision reference exactly, including functions whose values change at each new grant. `TestDynamicQuantumExtremeGrants` reaches the same peak through the optimistic path.

Changing a source never changes credit. A callback result is checked against the same `[1, MaxQuantum]` bounds BEFORE its local grant, so the original bound is unchanged. Function sources on empty queues receive no grants. If any busy queue has a function, production never multiplies a sampled quantum over skipped rounds: it executes every grant separately, since future function values need not be equal. This is intentionally slower for huge costs and tiny dynamic quanta; the bounded dynamic reference inputs avoid billions of rounds.

## Callable scheduling and concurrency

A callable `Take(n)` copies metadata for every queue and only the first
`min(n, queue length)` costs per queue, retaining actual queue lengths. At most
`n` total items can be selected, so those prefixes suffice, while actual lengths
prevent a truncated prefix from falsely emptying a queue. No payload is copied
until commit. Prefix counts sum to at most the total queued items, avoiding an
`n * queue count` allocation-size overflow. Memory is
`O(queue count + sum min(n, queue length))`, plus the result/selection arrays.

Planning and policy evaluation occur outside every scheduler lock. The final
commit checks a generation that changes on every actual scheduler mutation,
including source changes and topology replacement. Generations cannot wrap;
exhaustion panics before mutation. A successful commit performs all removals and
installs final credit/cursor state under one lock. A stale plan returns
`ErrStateChanged` with a nil result; invalid callback values return their quantum
error first; panics propagate. None commits partial scheduling progress. There
are no internal callback retries, goroutines, or clocks.

Callback side effects and other goroutines' committed mutations cannot be
rolled back. Concurrent takes may invoke the same function concurrently, so the
application must synchronize captured state and decide whether retries are
appropriate. Tests coordinate exact conflict windows with channels/waitgroups,
not sleeps; a timeout only bounds the reentrancy deadlock regression. The fixed
path still runs under one mutex and cannot produce `ErrStateChanged`.

## Check it yourself: plant a bug

Good tests fail when the code is wrong. To see it:

1. Use a disposable copy, or save the current `dwrr.go` outside the repository
   before editing; do not discard uncommitted implementation work. Find the check in `takeFixed` that decides whether a queue can afford its next item:
   ```go
   if d.deficits[q] < head.Cost {
   ```
2. Change `<` to `<=`, so a queue with exactly enough credit is treated as unable to afford the item.
3. Run:
   ```sh
   go test -run TestDifferentialRandom
   ```
   Each failure shows what the scheduler returned (`got`), what the reference returned (`want`), and the sequence of operations that led there. You can replay that sequence by hand to see exactly where the two diverge.
4. Restore your saved copy of `dwrr.go`, or discard the disposable checkout.
   Do not use `git checkout dwrr.go` on a working tree whose changes you need.

Other bugs worth planting:
- remove the `- 1` in `fastForward`'s `(gap - 1) / d.quantums[i]`, so it skips one round too many;
- delete the line in `takeFixed` that resets an emptied queue's credit (`d.deficits[q] = 0`); `popHead` has the same reset for `RemoveHead`.

The differential test catches each of these.

## Adding tests

- **Behavior changes** go in the reference model first. Then the implementation must match it. If the change is intentional, explain why the reference changed in the commit message.
- **Failing inputs** found by the fuzzer are saved under `testdata/fuzz/FuzzDifferential/`. After fixing the bug, commit them, so every future `go test` run replays them.
- **Numeric limits.** Any change that affects the bound above needs an updated argument here and a boundary test that reaches the new extreme.
