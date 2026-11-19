// Package dwrr implements a cost-aware Deficit Weighted Round Robin scheduler.
// Quantum and item costs are positive uint64 values. Scheduling order is
// determined by queue visits and FIFO heads, not by downstream execution time.
package dwrr

import (
	"errors"
	"math"
	"math/bits"
	"sync"
)

const (
	// MaxCost is the largest accepted item cost.
	MaxCost = uint64(1<<63 - 1)
	// MaxQuantum is the largest accepted queue quantum.
	MaxQuantum = uint64(1<<63 - 1)
)

var (
	// ErrZeroQuantum reports a zero constant or callback quantum.
	ErrZeroQuantum = errors.New("dwrr: quantum must be positive")
	// ErrZeroCost is returned when Enqueue receives an item with zero cost.
	ErrZeroCost = errors.New("dwrr: item cost must be positive")
	// ErrCostTooLarge is returned when Enqueue receives a cost above MaxCost.
	ErrCostTooLarge = errors.New("dwrr: item cost exceeds MaxCost")
	// ErrQuantumTooLarge reports a constant or callback quantum above MaxQuantum.
	ErrQuantumTooLarge = errors.New("dwrr: quantum exceeds MaxQuantum")
	// ErrInvalidQuantum reports a negative, fractional, nonfinite constant or nil function.
	ErrInvalidQuantum = errors.New("dwrr: invalid quantum source")
	// ErrStateChanged means scheduling state changed while Take evaluated functions.
	// Take commits no selections or credit on this error; callers may retry.
	ErrStateChanged = errors.New("dwrr: state changed during Take")
)

// Item is a payload and its positive scheduling cost. Enqueue copies the item
// record, so later changes to the caller's Cost do not affect scheduling.
type Item[T any] struct {
	Value T      // Payload returned by Take.
	Cost  uint64 // Positive scheduling cost, fixed when Enqueue succeeds.
}

// Scheduled is an item selected from Queue in scheduler service order.
type Scheduled[T any] struct {
	Queue uint   // Index of the source queue.
	Value T      // Scheduled payload.
	Cost  uint64 // The item's declared scheduling cost.
}

// DWRR is a concurrent-safe cost-aware deficit round-robin scheduler. Do not
// copy it after first use. Its zero value is unusable; use NewDWRR.
type DWRR[T any] struct {
	mu           sync.Mutex
	queues       [][]Item[T]
	quantums     []uint64
	quantumFuncs []func() uint64
	dynamicCount int    // Configured functions, including empty queues.
	generation   uint64 // Never wraps: exhaustion panics before any mutation.
	// Outside an active visit, deficit is at most MaxCost-1: a visit closes
	// below its head's cost (at most MaxCost), and emptying resets credit.
	// RemoveHead can expose a cheaper head but never increases deficit.
	// A grant adds at most MaxQuantum, yielding MaxCost-1+MaxQuantum = 2^64-3,
	// attained by TestBoundaryPeakCreditAfterReweight. Fast-forward adds credit
	// only up to the head's cost minus 1. Plain uint64 arithmetic is exact.
	deficits  []uint64
	busy      []uint64 // One bit per nonempty queue; unused high bits are zero.
	busyCount int
	total     int  // Number of queued items, independent of payload type.
	cursor    int  // Next queue when idle; current queue while a visit is active.
	active    bool // Whether the current cursor visit has already received quantum.
}

// NewDWRR creates count empty queues, each with quantum 1. Queue indices start
// at zero and remain stable while their queues exist; AddQueue and RemoveQueue
// change the count, with RemoveQueue affecting only the last queue.
func NewDWRR[T any](count uint) *DWRR[T] {
	n := int(count)
	d := &DWRR[T]{queues: make([][]Item[T], n), quantums: make([]uint64, n), quantumFuncs: make([]func() uint64, n), deficits: make([]uint64, n), busy: make([]uint64, (n+63)/64)}
	for i := range d.quantums {
		d.quantums[i] = 1
	}
	return d
}

// SetQuantum configures a constant or a per-queue func() uint64. Builtin integer
// and floating-point types are accepted; floats must be finite whole numbers.
// Constants and function results must be in [1, MaxQuantum]. A nil function,
// negative, fractional, or nonfinite number returns ErrInvalidQuantum; zero
// returns ErrZeroQuantum; an oversized number returns ErrQuantumTooLarge.
// Unsupported types (including defined numeric/function types) panic. An invalid
// index panics first. Rejected sources leave state unchanged.
//
// Credit and an active visit survive changes. The source is used only at the
// next grant, never to resume an active visit. Functions run outside the lock
// during Take, once per planned new grant, including multiple grants in one
// Take. A failed Take can discard those grants, but not function side effects.
func (d *DWRR[T]) SetQuantum(queue uint, source any) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_ = d.quantums[queue] // Preserve index panic precedence.
	quantum, getQuantum, err := quantumSource(source)
	if err != nil {
		return err
	}
	if getQuantum == nil && d.quantumFuncs[queue] == nil && d.quantums[queue] == quantum {
		return nil
	}
	d.change()
	if d.quantumFuncs[queue] != nil {
		d.dynamicCount--
	}
	if getQuantum != nil {
		d.dynamicCount++
	}
	d.quantums[queue], d.quantumFuncs[queue] = quantum, getQuantum
	return nil
}

func quantumSource(source any) (uint64, func() uint64, error) {
	var signed int64
	var quantum uint64
	switch x := source.(type) {
	case func() uint64:
		if x == nil {
			return 0, nil, ErrInvalidQuantum
		}
		return 0, x, nil
	case int:
		signed = int64(x)
	case int8:
		signed = int64(x)
	case int16:
		signed = int64(x)
	case int32:
		signed = int64(x)
	case int64:
		signed = x
	case uint:
		quantum = uint64(x)
	case uint8:
		quantum = uint64(x)
	case uint16:
		quantum = uint64(x)
	case uint32:
		quantum = uint64(x)
	case uint64:
		quantum = x
	case uintptr:
		quantum = uint64(x)
	case float32:
		return floatQuantum(float64(x))
	case float64:
		return floatQuantum(x)
	default:
		panic("dwrr: unsupported quantum source")
	}
	if signed < 0 {
		return 0, nil, ErrInvalidQuantum
	}
	if signed > 0 {
		quantum = uint64(signed)
	}
	return quantum, nil, validateQuantum(quantum)
}

func floatQuantum(x float64) (uint64, func() uint64, error) {
	if math.IsNaN(x) || math.IsInf(x, 0) || x < 0 || math.Trunc(x) != x {
		return 0, nil, ErrInvalidQuantum
	}
	// float64(MaxQuantum) rounds UP to 2^63, which is not an accepted quantum.
	if x >= 0x1p63 {
		return 0, nil, ErrQuantumTooLarge
	}
	quantum := uint64(x)
	return quantum, nil, validateQuantum(quantum)
}

func validateQuantum(quantum uint64) error {
	if quantum == 0 {
		return ErrZeroQuantum
	}
	if quantum > MaxQuantum {
		return ErrQuantumTooLarge
	}
	return nil
}

// change invalidates optimistic plans. Exhaustion cannot alias an old snapshot.
// Call before mutating state, with d.mu held.
func (d *DWRR[T]) change() {
	if d.generation == ^uint64(0) {
		panic("dwrr: state generation exhausted")
	}
	d.generation++
}

// Enqueue validates the complete input before appending it atomically. New
// items join an active visit at the queue tail. Item records, including costs,
// are copied; values of T are shallow-copied. Zero cost returns ErrZeroCost;
// a cost above MaxCost returns ErrCostTooLarge. Invalid queue indices panic.
func (d *DWRR[T]) Enqueue(queue uint, items []Item[T]) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	queueItems := &d.queues[queue] // Preserve slice-index panic behavior.
	for _, item := range items {
		if item.Cost == 0 {
			return ErrZeroCost
		}
		if item.Cost > MaxCost {
			return ErrCostTooLarge
		}
	}
	if len(items) == 0 {
		return nil
	}
	d.change()
	wasEmpty := len(*queueItems) == 0
	*queueItems = append(*queueItems, items...)
	d.total += len(items)
	if wasEmpty {
		d.busy[queue/64] |= uint64(1) << (queue % 64)
		d.busyCount++
	}
	return nil
}

// AddQueue appends an empty queue with quantum 1 and returns its index. The
// index is stable while the queue exists; if the last queue is removed, a later
// AddQueue may reuse its index.
func (d *DWRR[T]) AddQueue() uint {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.change()
	index := uint(len(d.queues))
	d.queues = append(d.queues, nil)
	d.quantums = append(d.quantums, 1)
	d.quantumFuncs = append(d.quantumFuncs, nil)
	d.deficits = append(d.deficits, 0)
	if index%64 == 0 {
		d.busy = append(d.busy, 0)
	}
	return index
}

// RemoveQueue removes the last queue and its contents. Existing queue indices
// remain stable. Removing the active queue cancels that visit; cursor is kept
// in range (or reset to zero when no queues remain). Empty removal is a no-op.
func (d *DWRR[T]) RemoveQueue() {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.queues)
	if n == 0 {
		return
	}
	d.change()
	last := n - 1
	if d.quantumFuncs[last] != nil {
		d.dynamicCount--
	}
	d.quantumFuncs[last] = nil
	d.quantumFuncs = d.quantumFuncs[:last]
	if d.active && d.cursor == last {
		d.active = false
		d.cursor = 0
	}
	if len(d.queues[last]) > 0 {
		d.total -= len(d.queues[last])
		d.busy[last/64] &^= uint64(1) << (last % 64)
		d.busyCount--
	}
	d.busy = d.busy[:(last+63)/64]
	d.queues[last] = nil
	d.deficits[last] = 0
	d.queues = d.queues[:last]
	d.quantums = d.quantums[:last]
	d.deficits = d.deficits[:last]
	if last == 0 {
		d.cursor = 0
	} else if d.cursor >= last {
		d.cursor %= last
	}
}

// RemoveHead removes and returns a copy of the FIFO head. It returns the zero
// Item and false when empty. If items remain, deficit and active-visit state are
// preserved; emptying the queue resets deficit and cancels an active visit.
// Invalid indices panic.
func (d *DWRR[T]) RemoveHead(queue uint) (Item[T], bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	q := int(queue)
	if len(d.queues[q]) == 0 {
		return Item[T]{}, false
	}
	d.change()
	return d.popHead(q), true
}

// popHead removes the head of nonempty queue q. The caller holds d.mu.
func (d *DWRR[T]) popHead(q int) Item[T] {
	item := d.queues[q][0]
	clear(d.queues[q][:1])
	if len(d.queues[q]) == 1 {
		d.queues[q] = d.queues[q][:0]
	} else {
		d.queues[q] = d.queues[q][1:]
	}
	d.total--
	if len(d.queues[q]) == 0 {
		d.busy[q/64] &^= uint64(1) << (q % 64)
		d.busyCount--
		d.deficits[q] = 0
		if d.active && d.cursor == q {
			d.closeVisit()
		}
	}
	return item
}

// DrainQueue removes and returns a copy of every queued item, resets deficit,
// and cancels an active visit. Invalid indices panic.
func (d *DWRR[T]) DrainQueue(queue uint) []Item[T] {
	d.mu.Lock()
	defer d.mu.Unlock()
	q := int(queue)
	if d.queues[q] != nil {
		d.change()
	}
	items := append([]Item[T](nil), d.queues[q]...)
	if len(d.queues[q]) > 0 {
		d.total -= len(d.queues[q])
		d.busy[q/64] &^= uint64(1) << (q % 64)
		d.busyCount--
	}
	clear(d.queues[q])
	d.queues[q] = nil
	d.deficits[q] = 0
	if d.active && d.cursor == q {
		d.closeVisit()
	}
	return items
}

// Take returns up to n items in service order, preserving unfinished visits.
// Take(0) returns a nonnil empty slice without evaluating sources or locking;
// negative n panics. Empty schedulers return a nonnil empty slice and nil error.
//
// If any busy queue has a function source, Take plans speculatively, invoking
// functions outside all scheduler locks on each new grant. Invalid results
// return a quantum error; concurrent state changes return ErrStateChanged.
// Either error returns nil and commits no selections, credit, or cursor changes.
// Function panics propagate with the same no-commit guarantee. Application
// side effects (including changes made by a function) cannot be rolled back.
// Calls are speculative: their grants may never commit. No automatic retry
// occurs. Callers must synchronize captured function state;
// concurrent Takes may invoke the same function concurrently.
func (d *DWRR[T]) Take(n int) ([]Scheduled[T], error) {
	if n < 0 {
		panic("dwrr: negative Take limit")
	}
	if n == 0 {
		return make([]Scheduled[T], 0), nil
	}
	d.mu.Lock()
	if !d.hasDynamicBusy() {
		defer d.mu.Unlock()
		if d.total > 0 {
			d.change()
		}
		return d.takeFixed(n), nil
	}
	plan := d.plan(n)
	generation := d.generation
	d.mu.Unlock()
	if err := plan.run(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.generation != generation {
		return nil, ErrStateChanged
	}
	d.change()
	out := make([]Scheduled[T], 0, len(plan.order))
	for _, q := range plan.order {
		head := d.popHead(q)
		out = append(out, Scheduled[T]{Queue: uint(q), Value: head.Value, Cost: head.Cost})
	}
	for q := range plan.queues {
		d.deficits[q] = plan.queues[q].deficit
	}
	d.cursor, d.active = plan.cursor, plan.active
	return out, nil
}

// hasDynamicBusy ignores callable sources on empty queues. Caller holds d.mu.
func (d *DWRR[T]) hasDynamicBusy() bool {
	if d.dynamicCount == 0 {
		return false
	}
	for word, busy := range d.busy {
		for busy != 0 {
			q := word*64 + bits.TrailingZeros64(busy)
			busy &= busy - 1
			if d.quantumFuncs[q] != nil {
				return true
			}
		}
	}
	return false
}

// takeFixed uses stable constants for all busy queues. Caller holds d.mu.
func (d *DWRR[T]) takeFixed(n int) []Scheduled[T] {
	out := make([]Scheduled[T], 0, min(n, d.total))
	// Count grants since service, excluding a visit already active on entry.
	grantsSinceService := 0
	for len(out) < n && d.busyCount > 0 {
		if !d.active {
			// Empty queues have zero credit, so their visits only advance the
			// cursor. Skip them without changing the order of busy visits.
			d.cursor = d.nextBusy(d.cursor)
			d.deficits[d.cursor] += d.quantums[d.cursor]
			d.active = true
			grantsSinceService++
		} else {
			q := d.cursor
			head := d.queues[q][0]
			if d.deficits[q] < head.Cost {
				d.closeVisit()
			} else {
				d.deficits[q] -= head.Cost
				// Keep removal local on this hot path: calling popHead per item
				// regressed scheduling benchmarks when the helper did not inline.
				head = d.queues[q][0]
				clear(d.queues[q][:1])
				if len(d.queues[q]) == 1 {
					d.queues[q] = d.queues[q][:0]
				} else {
					d.queues[q] = d.queues[q][1:]
				}
				d.total--
				if len(d.queues[q]) == 0 {
					d.busy[q/64] &^= uint64(1) << (q % 64)
					d.busyCount--
					d.deficits[q] = 0
					if d.active && d.cursor == q {
						d.closeVisit()
					}
				}
				out = append(out, Scheduled[T]{Queue: uint(q), Value: head.Value, Cost: head.Cost})
				grantsSinceService = 0
			}
		}
		if !d.active && grantsSinceService > 0 && grantsSinceService >= d.busyCount {
			d.fastForward()
			grantsSinceService = 0
		}
	}
	return out
}

// A plan copies costs only, not payloads. At most n heads per queue can be
// removed, but remaining tracks the true length so truncated prefixes never
// incorrectly empty a queue or discard its credit. Caller holds d.mu in plan.
type plannedQueue struct {
	costs               []uint64
	remaining, consumed int
	quantum, deficit    uint64
	getQuantum          func() uint64
}
type takePlan struct {
	queues        []plannedQueue
	order         []int
	limit, cursor int
	active        bool
}

func (d *DWRR[T]) plan(n int) takePlan {
	limit := min(n, d.total)
	prefixCount := 0
	for _, queue := range d.queues {
		prefixCount += min(limit, len(queue)) // Sum is at most d.total.
	}
	costs := make([]uint64, 0, prefixCount)
	p := takePlan{queues: make([]plannedQueue, len(d.queues)), order: make([]int, 0, limit), limit: limit, cursor: d.cursor, active: d.active}
	for q, queue := range d.queues {
		start := len(costs)
		for _, item := range queue[:min(limit, len(queue))] {
			costs = append(costs, item.Cost)
		}
		p.queues[q] = plannedQueue{costs: costs[start:len(costs)], remaining: len(queue), quantum: d.quantums[q], deficit: d.deficits[q], getQuantum: d.quantumFuncs[q]}
	}
	return p
}

func (p *takePlan) close() {
	p.active = false
	p.cursor = (p.cursor + 1) % len(p.queues)
}

func (p *takePlan) run() error {
	for len(p.order) < p.limit {
		q := &p.queues[p.cursor]
		if !p.active {
			if q.remaining == 0 {
				p.close()
				continue
			}
			quantum := q.quantum
			if q.getQuantum != nil {
				quantum = q.getQuantum()
				if err := validateQuantum(quantum); err != nil {
					return err
				}
			}
			q.deficit += quantum
			p.active = true
		}
		cost := q.costs[q.consumed]
		if q.deficit < cost {
			p.close()
			continue
		}
		q.deficit -= cost
		q.remaining--
		q.consumed++
		p.order = append(p.order, p.cursor)
		if q.remaining == 0 {
			q.deficit = 0
			p.close()
		}
	}
	return nil
}

// nextBusy finds the first nonempty queue at or after from, wrapping once.
func (d *DWRR[T]) nextBusy(from int) int {
	if d.busyCount == 0 {
		return -1
	}
	word := from / 64
	if candidates := d.busy[word] & (^uint64(0) << (from % 64)); candidates != 0 {
		return word*64 + bits.TrailingZeros64(candidates)
	}
	for i := word + 1; i < len(d.busy); i++ {
		if d.busy[i] != 0 {
			return i*64 + bits.TrailingZeros64(d.busy[i])
		}
	}
	for i := 0; i < word; i++ {
		if d.busy[i] != 0 {
			return i*64 + bits.TrailingZeros64(d.busy[i])
		}
	}
	// With busyCount > 0, the remaining bit must precede from in this word.
	return word*64 + bits.TrailingZeros64(d.busy[word])
}
func (d *DWRR[T]) closeVisit() {
	d.cursor = (d.cursor + 1) % len(d.queues)
	d.active = false
}

// fastForward skips complete no-service cycles without making a head eligible.
// The caller guarantees busyCount > 0.
func (d *DWRR[T]) fastForward() {
	skip := ^uint64(0)
	for word, busy := range d.busy {
		for busy != 0 {
			i := word*64 + bits.TrailingZeros64(busy)
			busy &= busy - 1
			cost := d.queues[i][0].Cost
			if d.deficits[i] >= cost {
				return
			}
			gap := cost - d.deficits[i]
			cycles := (gap - 1) / d.quantums[i]
			if cycles < skip {
				skip = cycles
			}
		}
	}
	if skip == 0 {
		return
	}
	for word, busy := range d.busy {
		for busy != 0 {
			i := word*64 + bits.TrailingZeros64(busy)
			busy &= busy - 1
			// The minimum skip ensures skip*quantum <= gap-1 < 2^63
			// for every queue. Neither multiplication nor addition overflows.
			d.deficits[i] += skip * d.quantums[i]
		}
	}
}
