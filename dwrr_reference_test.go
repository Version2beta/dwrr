package dwrr

import (
	"fmt"
	"math/bits"
	"math/rand"
	"reflect"
	"testing"
)

// naiveDRR deliberately visits one queue at a time, with no skipping or
// production helpers. Values equal costs so its FIFO storage is costs only.
type naiveDRR struct {
	queues           [][]uint64
	quantum, deficit []uint64
	getQuantum       []func() uint64
	cursor           int
	active           bool
}

func newNaive(n int) *naiveDRR {
	r := &naiveDRR{queues: make([][]uint64, n), quantum: make([]uint64, n), deficit: make([]uint64, n), getQuantum: make([]func() uint64, n)}
	for i := range r.quantum {
		r.quantum[i] = 1
	}
	return r
}
func (r *naiveDRR) close() { r.active = false; r.cursor = (r.cursor + 1) % len(r.queues) }
func (r *naiveDRR) take(n int) []Scheduled[uint64] {
	out := make([]Scheduled[uint64], 0)
	for len(out) < n {
		empty := true
		for _, q := range r.queues {
			if len(q) > 0 {
				empty = false
				break
			}
		}
		if empty {
			break
		}
		q := r.cursor
		if !r.active {
			if len(r.queues[q]) == 0 {
				r.deficit[q] = 0
				r.cursor = (q + 1) % len(r.queues)
				continue
			}
			x := r.quantum[q]
			if r.getQuantum[q] != nil {
				x = r.getQuantum[q]()
			}
			r.deficit[q] += x
			r.active = true
		}
		if len(r.queues[q]) == 0 {
			r.deficit[q] = 0
			r.close()
			continue
		}
		cost := r.queues[q][0]
		if r.deficit[q] < cost {
			r.close()
			continue
		}
		r.deficit[q] -= cost
		out = append(out, Scheduled[uint64]{uint(q), cost, cost})
		r.queues[q] = r.queues[q][1:]
		if len(r.queues[q]) == 0 {
			r.deficit[q] = 0
			r.close()
		}
	}
	return out
}
func (r *naiveDRR) setQuantum(q int, x uint64) error {
	if x == 0 {
		return ErrZeroQuantum
	}
	r.getQuantum[q] = nil
	r.quantum[q] = x // Credit and an active visit survive reweighting.
	return nil
}
func (r *naiveDRR) remove(q int) (Item[uint64], bool) {
	if len(r.queues[q]) == 0 {
		return Item[uint64]{}, false
	}
	cost := r.queues[q][0]
	r.queues[q] = r.queues[q][1:]
	if len(r.queues[q]) == 0 {
		r.deficit[q] = 0
		if r.active && r.cursor == q {
			r.close()
		}
	}
	return Item[uint64]{cost, cost}, true
}
func (r *naiveDRR) drain(q int) []Item[uint64] {
	var out []Item[uint64]
	for _, cost := range r.queues[q] {
		out = append(out, Item[uint64]{cost, cost})
	}
	r.queues[q] = nil
	r.deficit[q] = 0
	if r.active && r.cursor == q {
		r.close()
	}
	return out
}
func (r *naiveDRR) add() {
	r.queues = append(r.queues, nil)
	r.quantum = append(r.quantum, 1)
	r.deficit = append(r.deficit, 0)
	r.getQuantum = append(r.getQuantum, nil)
}
func (r *naiveDRR) removeQueue() {
	if len(r.queues) == 0 {
		return
	}
	last := len(r.queues) - 1
	if r.active && r.cursor == last {
		r.active = false
		r.cursor = 0
	}
	r.queues = r.queues[:last]
	r.quantum = r.quantum[:last]
	r.deficit = r.deficit[:last]
	r.getQuantum = r.getQuantum[:last]
	if last == 0 {
		r.cursor = 0
	} else if r.cursor >= last {
		r.cursor %= last
	}
}

// Every decoded operation is bounded: at most six queues and two appended
// costs in [1,32]. No reference execution needs arithmetic fast-forwarding.
func runDifferential(t *testing.T, data []byte) {
	t.Helper()
	runDifferentialOperations(t, data, false)
}

func runDifferentialOperations(t *testing.T, data []byte, adaptive bool) {
	t.Helper()
	if len(data) == 0 {
		return
	}
	n := 1 + int(data[0]%6)
	d, r := NewDWRR[uint64](uint(n)), newNaive(n)
	trace := []string{fmt.Sprintf("New(%d)", n)}
	var gotEvents, wantEvents []quantumEvent
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("%s\noperations:\n%v", fmt.Sprintf(format, args...), trace)
	}
	check := func() {
		t.Helper()
		if len(d.queues) > 0 && (d.cursor < 0 || d.cursor >= len(d.queues)) {
			fail("real cursor out of range")
		}
		if len(r.queues) > 0 && (r.cursor < 0 || r.cursor >= len(r.queues)) {
			fail("reference cursor out of range")
		}
		if d.active && (len(d.queues) == 0 || len(d.queues[d.cursor]) == 0) {
			fail("real active empty queue")
		}
		if r.active && (len(r.queues) == 0 || len(r.queues[r.cursor]) == 0) {
			fail("reference active empty queue")
		}
		if !reflect.DeepEqual(d.deficits, r.deficit) || d.cursor != r.cursor || d.active != r.active || !reflect.DeepEqual(gotEvents, wantEvents) {
			fail("state/events mismatch: deficit=%v/%v cursor=%d/%d active=%t/%t events=%v/%v", d.deficits, r.deficit, d.cursor, r.cursor, d.active, r.active, gotEvents, wantEvents)
		}
		dynamicCount := 0
		for q := range r.quantum {
			if d.quantumFuncs[q] != nil {
				dynamicCount++
			}
			if (d.quantumFuncs[q] == nil) != (r.getQuantum[q] == nil) || (r.getQuantum[q] == nil && d.quantums[q] != r.quantum[q]) {
				fail("quantum source mismatch at %d", q)
			}
		}
		if d.dynamicCount != dynamicCount {
			fail("dynamic count=%d want=%d", d.dynamicCount, dynamicCount)
		}
		busyCount, total, setBits := 0, 0, 0
		for _, word := range d.busy {
			setBits += bits.OnesCount64(word)
		}
		for q := range r.queues {
			nonempty := len(d.queues[q]) > 0
			if bit := d.busy[q/64] & (uint64(1) << (q % 64)); (bit != 0) != nonempty {
				fail("busy bit mismatch at queue %d", q)
			}
			if nonempty {
				busyCount++
			}
			total += len(d.queues[q])
			if len(d.queues[q]) == 0 && d.deficits[q] != 0 {
				fail("real empty queue %d has credit", q)
			}
			if len(r.queues[q]) == 0 && r.deficit[q] != 0 {
				fail("reference empty queue %d has credit", q)
			}
		}
		if d.busyCount != busyCount || d.busyCount != setBits || d.total != total {
			fail("busy/total mismatch: count=%d want=%d bits=%d total=%d want=%d", d.busyCount, busyCount, setBits, d.total, total)
		}
	}
	check()
	for i := 1; i+3 < len(data); i += 4 {
		op, a, b, c := data[i]%9, data[i+1], data[i+2], data[i+3]
		// The first pass preserves the original interpretation of EVERY byte.
		// Only the adaptive second pass treats literal opcode 9 as a function.
		if adaptive && data[i] == 9 {
			op = 9
		}
		if len(r.queues) == 0 && ((op >= 3 && op <= 6) || op == 9) {
			trace = append(trace, "AddQueue() [indexed operation]")
			d.AddQueue()
			r.add()
		}
		q := 0
		if len(r.queues) > 0 {
			q = int(a) % len(r.queues)
		}
		switch op {
		case 0, 1, 2:
			n := int(b % 6)
			trace = append(trace, fmt.Sprintf("Take(%d)", n))
			if got, want := takeN(d, n), r.take(n); !reflect.DeepEqual(got, want) {
				fail("Take got=%v want=%v", got, want)
			}
		case 3:
			costs := []uint64{1 + uint64(b%32)}
			if c&1 != 0 {
				costs = append(costs, 1+uint64(c%32))
			}
			trace = append(trace, fmt.Sprintf("Enqueue(%d,%v)", q, costs))
			items := make([]Item[uint64], len(costs))
			for j, cost := range costs {
				items[j] = Item[uint64]{cost, cost}
			}
			if err := d.Enqueue(uint(q), items); err != nil {
				fail("Enqueue: %v", err)
			}
			r.queues[q] = append(r.queues[q], costs...)
		case 4:
			trace = append(trace, fmt.Sprintf("RemoveHead(%d)", q))
			got, ok := d.RemoveHead(uint(q))
			want, wok := r.remove(q)
			if got != want || ok != wok {
				fail("RemoveHead got=(%v,%t) want=(%v,%t)", got, ok, want, wok)
			}
		case 5:
			trace = append(trace, fmt.Sprintf("DrainQueue(%d)", q))
			if got, want := d.DrainQueue(uint(q)), r.drain(q); !reflect.DeepEqual(got, want) {
				fail("DrainQueue got=%v want=%v", got, want)
			}
		case 6:
			x := uint64(b % 9)
			trace = append(trace, fmt.Sprintf("SetQuantum(%d,%d)", q, x))
			if got, want := d.SetQuantum(uint(q), x), r.setQuantum(q, x); got != want {
				fail("SetQuantum got=%v want=%v", got, want)
			}
		case 7:
			if len(r.queues) == 6 {
				break
			}
			trace = append(trace, "AddQueue()")
			if got := d.AddQueue(); got != uint(len(r.queues)) {
				fail("AddQueue index=%d", got)
			}
			r.add()
		case 8:
			trace = append(trace, "RemoveQueue()")
			d.RemoveQueue()
			r.removeQueue()
		case 9:
			trace = append(trace, fmt.Sprintf("SetQuantum(%d,script(%d,%d))", q, b, c))
			if err := d.SetQuantum(uint(q), scriptedQuantum(q, b, c, &gotEvents)); err != nil {
				fail("function source: %v", err)
			}
			r.getQuantum[q] = scriptedQuantum(q, b, c, &wantEvents)
		}
		check()
	}
}

func TestDifferentialRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(0xD122))
	for trial := 0; trial < 3000; trial++ {
		data := make([]byte, 1+4*100)
		_, _ = rng.Read(data)
		t.Run(fmt.Sprint(trial), func(t *testing.T) { runDifferential(t, data) })
	}
}

type quantumEvent struct {
	queue   int
	quantum uint64
}

// Each scheduler gets its OWN script state; sharing a closure would hide
// skipped calls or make one scheduler consume the other's future values.
func scriptedQuantum(q int, start, step byte, events *[]quantumEvent) func() uint64 {
	calls := uint64(0)
	return func() uint64 {
		x := 1 + (uint64(start)+calls*uint64(step))%9
		calls = (calls + 1) % 9
		*events = append(*events, quantumEvent{q, x})
		return x
	}
}

func TestDynamicDifferentialScenarios(t *testing.T) {
	for name, data := range map[string][]byte{
		"per-grant-not-per-batch": {1, 9, 0, 0, 8, 9, 1, 1, 0, 3, 0, 9, 0, 3, 1, 5, 0, 0, 0, 2, 0},
		"active-source-changes":   {1, 3, 0, 0, 7, 3, 1, 31, 0, 6, 0, 8, 0, 0, 0, 1, 0, 9, 0, 1, 3, 0, 0, 1, 0, 6, 0, 2, 0, 0, 0, 5, 0},
		"withdraw-empty-reuse":    {1, 9, 0, 4, 1, 3, 0, 31, 1, 0, 0, 1, 0, 4, 0, 0, 0, 5, 0, 0, 0, 9, 1, 2, 7, 8, 0, 0, 0, 7, 0, 0, 0, 3, 1, 23, 1, 0, 0, 5, 0},
	} {
		t.Run(name, func(t *testing.T) { runDifferentialOperations(t, data, true) })
	}
}

func TestDynamicDifferentialRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(0xADA971))
	for trial := 0; trial < 2000; trial++ {
		data := make([]byte, 1+4*100)
		_, _ = rng.Read(data)
		for i := 1; i+3 < len(data); i += 4 {
			data[i] = byte(rng.Intn(10))
		}
		t.Run(fmt.Sprint(trial), func(t *testing.T) { runDifferentialOperations(t, data, true) })
	}
}

func FuzzDifferential(f *testing.F) {
	// Each operation is {opcode, queue, argument, tail}; initial byte is n-1.
	f.Add([]byte{1, 6, 0, 4, 0, 3, 0, 0, 1, 0, 0, 1, 0})                                     // paused visit
	f.Add([]byte{1, 6, 0, 4, 0, 3, 0, 0, 1, 0, 0, 1, 0, 3, 0, 0, 0, 0, 0, 3, 0})             // append during visit
	f.Add([]byte{1, 6, 0, 4, 0, 3, 0, 0, 7, 3, 0, 0, 0, 0, 0, 1, 0, 4, 0, 0, 0, 0, 0, 1, 0}) // cheaper exposed head
	f.Add([]byte{1, 3, 1, 0, 1, 0, 0, 1, 0, 8, 0, 0, 0})                                     // remove active last queue
	f.Add([]byte{1, 3, 0, 31, 0, 3, 1, 23, 0, 0, 0, 2, 0})                                   // every head exceeds quantum
	f.Add([]byte{1, 9, 0, 0, 8, 9, 1, 1, 0, 3, 0, 9, 0, 3, 1, 5, 0, 0, 0, 2, 0})             // per-grant functions
	f.Fuzz(func(t *testing.T, data []byte) {
		runDifferential(t, data)
		runDifferentialOperations(t, data, true)
	})
}

const boundaryMax = MaxCost // MaxQuantum has the same limit.

func TestBoundaryMaximumCostAndQuantum(t *testing.T) {
	d := NewDWRR[int](1)
	mustQuantum(t, d, 0, boundaryMax)
	mustEnqueue(t, d, 0, []Item[int]{{1, boundaryMax}, {2, boundaryMax}})
	if got, want := takeN(d, 2), ([]Scheduled[int]{{0, 1, boundaryMax}, {0, 2, boundaryMax}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
func TestBoundaryHugeSkipServiceOrder(t *testing.T) {
	d := NewDWRR[int](3)
	for q := uint(0); q < 3; q++ {
		mustEnqueue(t, d, q, []Item[int]{{int(q), boundaryMax - uint64(q)}})
	}
	want := []Scheduled[int]{{2, 2, boundaryMax - 2}, {1, 1, boundaryMax - 1}, {0, 0, boundaryMax}}
	if got := takeN(d, 3); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
func TestBoundaryPausedCreditAndRemoval(t *testing.T) {
	d := NewDWRR[int](2)
	mustQuantum(t, d, 0, boundaryMax)
	mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, boundaryMax}, {3, 1}})
	mustEnqueue(t, d, 1, []Item[int]{{9, 1}})
	if got := takeN(d, 1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 1, 1}}) {
		t.Fatal(got)
	}
	if got, ok := d.RemoveHead(0); !ok || got.Value != 2 {
		t.Fatal(got, ok)
	}
	if got := takeN(d, 2); !reflect.DeepEqual(got, []Scheduled[int]{{0, 3, 1}, {1, 9, 1}}) {
		t.Fatal(got)
	}
}
func TestBoundaryRangeErrorsPreserveState(t *testing.T) {
	for _, take := range []int{1, 2} {
		t.Run(fmt.Sprintf("Take%d", take), func(t *testing.T) {
			d := NewDWRR[int](2)
			mustQuantum(t, d, 0, 3)
			mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, 10}})
			mustEnqueue(t, d, 1, []Item[int]{{9, 1}, {10, 1}})
			takeN(d, take)
			before := cloneForTest(d)
			if err := d.SetQuantum(0, MaxQuantum+1); err != ErrQuantumTooLarge {
				t.Fatalf("oversized quantum: %v", err)
			}
			if err := d.SetQuantum(0, 0); err != ErrZeroQuantum {
				t.Fatalf("zero quantum: %v", err)
			}
			if err := d.Enqueue(0, []Item[int]{{3, 1}, {4, MaxCost + 1}}); err != ErrCostTooLarge {
				t.Fatalf("oversized cost: %v", err)
			}
			if !reflect.DeepEqual(d.queues, before.queues) || !reflect.DeepEqual(d.quantums, before.quantums) || !reflect.DeepEqual(d.deficits, before.deficits) || d.cursor != before.cursor || d.active != before.active {
				t.Fatal("rejected input changed scheduling state")
			}
			if got, want := takeN(d, 10), takeN(before, 10); !reflect.DeepEqual(got, want) {
				t.Fatalf("rejection changed service order: got %v want %v", got, want)
			}
		})
	}
	for _, operation := range []func(){
		func() { NewDWRR[int](0).SetQuantum(0, MaxQuantum+1) },
		func() { NewDWRR[int](0).Enqueue(0, []Item[int]{{1, MaxCost + 1}}) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("invalid index did not panic before range validation")
				}
			}()
			operation()
		}()
	}
}

func TestBoundaryFastForwardAllocations(t *testing.T) {
	d := NewDWRR[int](8)
	for q := uint(0); q < 8; q++ {
		mustEnqueue(t, d, q, []Item[int]{{int(q), MaxCost - uint64(q)}})
	}
	allocations := testing.AllocsPerRun(100, func() {
		clear(d.deficits)
		d.fastForward()
	})
	if allocations != 0 {
		t.Fatalf("fastForward allocated %g times, want zero", allocations)
	}
}

func TestBoundaryLaterMinimumGap(t *testing.T) {
	d := NewDWRR[int](3)
	for q, cost := range []uint64{31, 23, 9} {
		mustQuantum(t, d, uint(q), 2)
		mustEnqueue(t, d, uint(q), []Item[int]{{q, cost}})
	}
	want := []Scheduled[int]{{2, 2, 9}, {1, 1, 23}, {0, 0, 31}}
	if got := takeN(d, 3); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
