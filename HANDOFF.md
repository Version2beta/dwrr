# DWRR adaptive quantum handoff

## Repository and state

Repository: `/Users/rob/Development/dwrr` (`github.com/version2beta/dwrr`).
Base/current HEAD at handoff: `492dc7e3e54f430212f483a728957ff7f151b4a5`.
Implementation, tests and documentation are **uncommitted**. Nothing was staged,
committed, pushed, tagged or released. No dependencies or version numbers changed.
Inspect actual status/diffs before continuing; preserve all existing work.

This work began in the conversation used for the separate `cxs` readme-loop.
DWRR is **not** a readme-loop kit. Do not run the cxs implementation loop here.
The completed DWRR changes are in this repository; pending cxs review is separate.

## Objective and final design

General-purpose deficit weighted round robin with application-defined, per-queue
quantum functions. The motivating application is deadline-aware MPEG-TS service,
but the core has no media types, deadlines, clocks, background updates or goroutines.

The design evolved during discussion. The original numeric-vector snapshot API
was **not implemented**. Backward compatibility was explicitly relaxed. Final
semantics retain batched `Take(n)` but evaluate a queue's function whenever it
needs a **new credit grant**, possibly multiple times in one batch. Do not revert
to one callback evaluation per batch. A function returning a constant is still
not assumed constant. A queue stops participating when it becomes empty, not
when its quantum becomes zero; zero remains invalid.

Implemented API:

```go
func (d *DWRR[T]) SetQuantum(queue uint, source any) error
func (d *DWRR[T]) Take(n int) ([]Scheduled[T], error)
```

- Sources: builtin integer types, finite whole-valued floats, or `func() uint64`.
  Method values such as `stream.GetQuantum` work directly.
- Constants and callback results must be in `[1, MaxQuantum]`.
- Invalid numeric values and nil functions return errors; unsupported types,
  including defined numeric/function types, panic. Type aliases work as their
  underlying builtin type. Invalid queue indices panic first.
- Fractional floats are rejected, not rounded. `float64(MaxQuantum)` rounds to
  `2^63` and is therefore rejected; integer input can represent MaxQuantum exactly.
- Item costs remain positive fixed uint64 values. Credit is exact and survives
  quantum/source changes. An active visit spends existing credit; a new source
  takes effect at the next grant. Empty queues discard credit.
- `Take(0)` returns a nonnil empty slice and nil error without evaluating policies
  or locking. Negative n panics. Successful empty retrievals are nonnil slices.

## Scheduling, callbacks and fast-forward

Fixed-only busy queues keep the existing mutex-protected, optimized scheduling
path, including arithmetic fast-forward. Functions on empty queues do not disable
that path.

If any busy queue has a function, Take copies scheduling metadata and up-to-n
FIFO cost prefixes, retaining actual queue lengths. It plans a batch outside
**all scheduler locks**, invoking functions at each new grant. No dynamic grants
are skipped or extrapolated. Commit takes the mutex and succeeds only if the
scheduler generation still matches the snapshot.

- A stale plan returns `ErrStateChanged`; invalid results return a quantum error.
  Errors return a nil result without applying the plan's credit, cursor or removals.
- Callback panics propagate without committing the plan.
- No callbacks are retried automatically. Callers decide whether to retry Take.
- Application side effects, including callback-initiated scheduler mutations,
  cannot be rolled back. Concurrent Takes can invoke the same function concurrently;
  the application owns synchronization of captured state.
- Every relevant mutation invalidates plans, including remove/re-add with unchanged
  queue count. Generation exhaustion panics before mutation rather than wrapping.
- Dynamic planning uses `O(queue count + sum min(n, queue length))` memory plus
  selection/results. Huge costs with tiny dynamic quanta can require an impractical
  number of grants. This is an explicit cost of arbitrary per-grant evaluation.

## Files and coverage

- `dwrr.go`: source validation, per-queue functions, generations, fixed fast path,
  speculative per-grant planning and atomic commit.
- `dwrr_adaptive_test.go`: grant counts/order, paused visits, bounds, invalid sources,
  speculative rollback, reentrancy, concurrent conflicts/item conservation,
  prefix truncation and generation exhaustion.
- `dwrr_reference_test.go`: independent naive model with independent policy scripts
  and callback traces. Original 3,000 random scenarios and original fuzz decoding
  preserved; another 2,000 scenarios cover dynamic sources.
- `dwrr_extreme_test.go`: big-integer credit reference and varying per-grant policies.
- Existing fixed-source tests use a helper that unwraps ONE batched Take call;
  they were not converted into repeated single-item calls. Original 2,000 mutation
  scenarios remain.
- `dwrr_external_test.go`: changed public API and callback-result errors.
- `example_test.go`, `deadline_policy_test.go`: application-owned GetQuantum method,
  caller-supplied monotonic timestamps, deadline/ramp/min/max inputs, deterministic
  overflow-safe interpolation checked against an independent math/big oracle.
  This allocation policy does **not** guarantee deadlines.
- `dwrr_bench_test.go`: five existing benchmarks retained; dynamic benchmark added.
- `README.md`, `TESTING.md`: API, concurrency, arithmetic and testing documentation.

## Current local release-preparation evidence

These checks were rerun on the uncommitted implementation at the HEAD above,
with Go 1.27.2 on Darwin/arm64 (Apple M5). All Go commands used
`GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off`; no downloads, dependencies or version
numbers were changed. This evidence supersedes the earlier implementation-session
validation record; it is not a published release or remote CI result.

Passed:

```sh
git diff --check
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go test ./... -run 'TestDynamic|TestSetQuantumSources|TestDeadlineQuantum|TestPublic|TestGeneration|Example' -count=1 -v
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzDifferential$' -fuzztime=60s -parallel=2
go test -run '^$' -bench . -benchmem -benchtime=200ms -count=3
```

- Fuzz: 1,726,699 executions, 57 new interesting inputs; no failures or failing
  artifacts. The bounded run does not prove exhaustive correctness.
- All four standalone README examples ran in an external temporary consumer with
  a local module replacement; their output matched the documented text exactly.
  The callable example also passed as a Go example test.
- All six benchmarks passed with three short samples each. Dynamic backlogged
  scheduling measured 194.2–195.4 ns/op, 1,343 B/op and four allocations/op.
  No authoritative baseline was established; no regression/improvement claim is
  made. These fixtures do not establish application throughput or latency.
- Source/toolchain configuration and raw logs were captured outside the repository.
  The index remained empty; implementation and tests were left untouched by this
  documentation pass. Nothing was committed, tagged, pushed or published.
- Only Go 1.27.2 was found in the normal installed toolchain locations. Exact Go
  1.22.2/minimum compatibility is **unproven**. CI's 1.22.x/stable matrix was read,
  not run or queried; no unrelated caches were used to source a toolchain.

Documentation now labels the API as **Unreleased**, distinguishes published
v0.10.1, adds migration notes and replaces the misleading no-API-change changelog
entry. No next release version was selected.

## Remaining release gates

1. Final documentation/evidence review completed with no issues found. The
   read-only reviewer checked API/documentation consistency and spot-checked the
   current raw validation logs. This bounded review does not replace maintainer
   acceptance of the implementation or authorize publication.
2. Validate the declared Go 1.22.2 minimum and the supported CI matrix on the
   intended release commit. Revalidate after any source changes.
3. Confirm application acceptance of error-returning Take, speculative concurrent
   callbacks, retry handling and dynamic overhead. No live media integration or
   deadline guarantee was established.
4. With separate authorization, select a version, finalize release documentation,
   review/commit the intended changes and obtain green CI on that exact commit.
5. Only with publication authorization, tag/push and verify the exact version via
   the module proxy, a clean consumer and pkg.go.dev. Follow RELEASING.md; none of
   these publication steps was performed locally.

## Cross-language consistency follow-up

The shared Go driver now adapts both published v0.10.1 and this checkout's
error-returning Take without changing its module pin. Fresh isolated offline
builds of both replayed all 1,839 cases / 44,728 steps and passed the expanded
41-check protocol checker. Current-source Elixir passed the same checks; Python
remains documentation-only with D1–D5 unapproved. Generation exhaustion remains
unchanged and explicitly qualifies current-Go fixed-core all-sequences
conformance. The shared driver now parses constructor counts as strict unbounded
JSON integers: valid nonnegative counts above native int maximum terminate before
narrowing/allocation as resource exhaustion (stderr, exit 1, no response or later
requests; prior responses stand), not protocol errors (exit 2). This narrowly
approved representation policy adds no common-domain cap or shared error and
promises no controlled allocation/OOM recovery. The 64 MiB line restriction
remains a Go adapter qualification. No release was selected.
