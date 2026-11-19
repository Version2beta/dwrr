package dwrr

import (
	"math"
	"reflect"
	"sync"
	"testing"
	"time"
)

func assertSchedulingUnchanged(t *testing.T, d, before *DWRR[int]) {
	t.Helper()
	if !reflect.DeepEqual(d.queues, before.queues) || !reflect.DeepEqual(d.quantums, before.quantums) || !reflect.DeepEqual(d.deficits, before.deficits) || !reflect.DeepEqual(d.busy, before.busy) || d.cursor != before.cursor || d.active != before.active || d.total != before.total || d.busyCount != before.busyCount || d.dynamicCount != before.dynamicCount || d.generation != before.generation {
		t.Fatal("rejected operation committed scheduling state")
	}
	for q := range d.quantumFuncs {
		if (d.quantumFuncs[q] == nil) != (before.quantumFuncs[q] == nil) {
			t.Fatal("rejected operation changed source kind")
		}
	}
}

func TestSetQuantumSources(t *testing.T) {
	for _, source := range []any{int(2), int8(2), int16(2), int32(2), int64(2), uint(2), uint8(2), uint16(2), uint32(2), uint64(2), uintptr(2), float32(2), float64(2)} {
		d := NewDWRR[int](1)
		if err := d.SetQuantum(0, source); err != nil || d.quantums[0] != 2 || d.dynamicCount != 0 {
			t.Fatalf("%T: %v", source, err)
		}
	}
	for _, tc := range []struct {
		source any
		err    error
	}{
		{0, ErrZeroQuantum}, {float64(0), ErrZeroQuantum}, {math.Copysign(0, -1), ErrZeroQuantum},
		{-1, ErrInvalidQuantum}, {int64(-1), ErrInvalidQuantum}, {-0.5, ErrInvalidQuantum}, {1.5, ErrInvalidQuantum},
		{math.NaN(), ErrInvalidQuantum}, {math.Inf(1), ErrInvalidQuantum}, {math.Inf(-1), ErrInvalidQuantum},
		{MaxQuantum + 1, ErrQuantumTooLarge}, {^uint64(0), ErrQuantumTooLarge}, {float64(MaxQuantum), ErrQuantumTooLarge},
		{float32(0x1p63), ErrQuantumTooLarge}, {(func() uint64)(nil), ErrInvalidQuantum},
	} {
		d := NewDWRR[int](1)
		mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, 9}})
		mustQuantum(t, d, 0, 4)
		takeN(d, 1)
		before := cloneForTest(d)
		if err := d.SetQuantum(0, tc.source); err != tc.err {
			t.Errorf("%T(%v): got %v want %v", tc.source, tc.source, err, tc.err)
		}
		assertSchedulingUnchanged(t, d, before)
	}
	d := NewDWRR[int](1)
	if err := d.SetQuantum(0, math.Nextafter(0x1p63, 0)); err != nil || d.quantums[0] != uint64(math.Nextafter(0x1p63, 0)) {
		t.Fatal("largest in-range float rejected", err)
	}
	type namedInt int
	type namedFunc func() uint64
	for _, source := range []any{nil, true, "2", namedInt(2), namedFunc(func() uint64 { return 2 }), func() int { return 2 }} {
		before := cloneForTest(d)
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("unsupported source %T did not panic", source)
				}
			}()
			d.SetQuantum(0, source)
		}()
		assertSchedulingUnchanged(t, d, before)
	}
	func() {
		defer func() {
			if got := recover(); got == nil || got == "dwrr: unsupported quantum source" {
				t.Errorf("invalid index must panic first: %v", got)
			}
		}()
		NewDWRR[int](0).SetQuantum(0, "unsupported")
	}()
}

func TestDynamicQuantumEveryGrantWithinBatch(t *testing.T) {
	d := NewDWRR[int](2)
	var calls []int
	x := 0
	if err := d.SetQuantum(0, func() uint64 {
		calls = append(calls, 0)
		x++
		if x == 1 {
			return 1
		}
		return 9
	}); err != nil {
		t.Fatal(err)
	}
	if err := d.SetQuantum(1, func() uint64 { calls = append(calls, 1); return 2 }); err != nil {
		t.Fatal(err)
	}
	mustEnqueue(t, d, 0, []Item[int]{{10, 10}})
	mustEnqueue(t, d, 1, []Item[int]{{20, 6}})
	// A frozen quantum or fast-forward would incorrectly serve queue 1 first.
	want := []Scheduled[int]{{0, 10, 10}, {1, 20, 6}}
	if got := takeN(d, 2); !reflect.DeepEqual(got, want) || !reflect.DeepEqual(calls, []int{0, 1, 0, 1, 1}) {
		t.Fatal(got, calls)
	}
}

func TestDynamicQuantumActiveVisitAndSourceChanges(t *testing.T) {
	d := NewDWRR[int](2)
	mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, 3}, {3, 4}, {4, 1}})
	mustEnqueue(t, d, 1, []Item[int]{{9, 1}, {10, 1}})
	mustQuantum(t, d, 0, 5)
	takeN(d, 1)
	calls := 0
	if err := d.SetQuantum(0, func() uint64 { calls++; return 3 }); err != nil {
		t.Fatal(err)
	}
	if got := takeN(d, 1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 2, 3}}) || calls != 0 || d.deficits[0] != 1 {
		t.Fatal(got, calls, d.deficits)
	}
	if got := takeN(d, 1); !reflect.DeepEqual(got, []Scheduled[int]{{1, 9, 1}}) || calls != 0 {
		t.Fatal("unaffordable head must close active visit", got, calls)
	}
	if got := takeN(d, 1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 3, 4}}) || calls != 1 || d.deficits[0] != 0 {
		t.Fatal(got, calls, d.deficits)
	}
	mustQuantum(t, d, 0, 10) // dynamic -> fixed during the paused visit
	if got := takeN(d, 1); !reflect.DeepEqual(got, []Scheduled[int]{{1, 10, 1}}) || calls != 1 {
		t.Fatal(got, calls)
	}
	if got := takeN(d, 1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 4, 1}}) || calls != 1 {
		t.Fatal(got, calls)
	}
}

func TestDynamicQuantumPrefixAndEmptyQueues(t *testing.T) {
	d := NewDWRR[int](2)
	calls := 0
	if err := d.SetQuantum(0, func() uint64 { calls++; return 10 }); err != nil {
		t.Fatal(err)
	}
	mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, 1}, {3, 1}})
	if got := takeN(d, 1); len(got) != 1 || d.deficits[0] != 9 || !d.active || d.total != 2 || calls != 1 {
		t.Fatal("truncated snapshot prefix incorrectly emptied queue", got, d.deficits, calls)
	}
	if got := takeN(d, 2); len(got) != 2 || calls != 1 || d.deficits[0] != 0 || d.active {
		t.Fatal(got, calls)
	}
	// Empty callable queues are never evaluated and must not disable the
	// constant-only fast-forward path (this cost needs 2^63-1 grants otherwise).
	mustEnqueue(t, d, 1, []Item[int]{{9, MaxCost}})
	if got := takeN(d, math.MaxInt); !reflect.DeepEqual(got, []Scheduled[int]{{1, 9, MaxCost}}) || calls != 1 {
		t.Fatal(got, calls)
	}
	if out, err := d.Take(0); out == nil || len(out) != 0 || err != nil || calls != 1 {
		t.Fatal(out, err, calls)
	}
	if out, err := d.Take(1); out == nil || len(out) != 0 || err != nil || calls != 1 {
		t.Fatal(out, err, calls)
	}
	mustEnqueue(t, d, 0, []Item[int]{{4, 1}, {5, 1}})
	takeN(d, 1)
	d.RemoveHead(0)
	if d.deficits[0] != 0 || d.active {
		t.Fatal("remove last head retained credit")
	}
	mustEnqueue(t, d, 0, []Item[int]{{6, 1}, {7, 1}})
	takeN(d, 1)
	d.DrainQueue(0)
	if d.deficits[0] != 0 || d.active {
		t.Fatal("drain retained credit")
	}
	d.RemoveQueue()
	d.RemoveQueue()
	if d.dynamicCount != 0 || d.AddQueue() != 0 || d.quantumFuncs[0] != nil || d.quantums[0] != 1 {
		t.Fatal("removed source retained on reused index")
	}
}

func TestDynamicQuantumInvalidOrPanicNoPartialBatch(t *testing.T) {
	for _, result := range []uint64{0, MaxQuantum + 1} {
		d := NewDWRR[int](2)
		mustEnqueue(t, d, 0, []Item[int]{{1, 1}})
		mustEnqueue(t, d, 1, []Item[int]{{2, 1}})
		if err := d.SetQuantum(1, func() uint64 { return result }); err != nil {
			t.Fatal(err)
		}
		before := cloneForTest(d)
		want := ErrZeroQuantum
		if result != 0 {
			want = ErrQuantumTooLarge
		}
		if out, err := d.Take(2); out != nil || err != want {
			t.Fatal(out, err)
		}
		assertSchedulingUnchanged(t, d, before)
	}
	d := NewDWRR[int](2)
	mustEnqueue(t, d, 0, []Item[int]{{1, 1}})
	mustEnqueue(t, d, 1, []Item[int]{{2, 1}})
	_ = d.SetQuantum(1, func() uint64 { panic("policy panic") })
	before := cloneForTest(d)
	func() {
		defer func() {
			if got := recover(); got != "policy panic" {
				t.Errorf("panic did not propagate: %v", got)
			}
		}()
		d.Take(2)
	}()
	assertSchedulingUnchanged(t, d, before)
	mustQuantum(t, d, 1, 1) // A propagated panic must leave the mutex unlocked.
	if len(takeN(d, 2)) != 2 {
		t.Fatal("panic lost work")
	}
}

func TestDynamicQuantumReentrantChange(t *testing.T) {
	d := NewDWRR[int](1)
	mustEnqueue(t, d, 0, []Item[int]{{1, 3}})
	calls := 0
	_ = d.SetQuantum(0, func() uint64 {
		calls++
		if err := d.SetQuantum(0, 7); err != nil {
			panic(err)
		}
		return 3
	})
	done := make(chan error, 1)
	go func() {
		out, err := d.Take(1)
		if out != nil {
			done <- ErrInvalidQuantum
			return
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != ErrStateChanged || calls != 1 || d.quantums[0] != 7 || d.deficits[0] != 0 || d.total != 1 || d.active {
			t.Fatal("callback change must persist, plan must not", err, calls, d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("callback deadlocked on scheduler lock")
	}
}

func TestDynamicQuantumConcurrentMutationsRejectPlan(t *testing.T) {
	for name, mutate := range map[string]func(*DWRR[int]){
		"enqueue":        func(d *DWRR[int]) { _ = d.Enqueue(0, []Item[int]{{3, 1}}) },
		"quantum":        func(d *DWRR[int]) { _ = d.SetQuantum(0, 2) },
		"removeHead":     func(d *DWRR[int]) { d.RemoveHead(0) },
		"drain":          func(d *DWRR[int]) { d.DrainQueue(0) },
		"add":            func(d *DWRR[int]) { d.AddQueue() },
		"removeAndReuse": func(d *DWRR[int]) { d.RemoveQueue(); d.AddQueue() },
		"take":           func(d *DWRR[int]) { _ = d.SetQuantum(1, 1); takeN(d, 1) },
	} {
		t.Run(name, func(t *testing.T) {
			d := NewDWRR[int](2)
			mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, 1}})
			mustEnqueue(t, d, 1, []Item[int]{{9, 1}})
			entered, release := make(chan struct{}), make(chan struct{})
			_ = d.SetQuantum(1, func() uint64 { close(entered); <-release; return 1 })
			done := make(chan error, 1)
			go func() {
				out, err := d.Take(3)
				if out != nil {
					done <- ErrInvalidQuantum
					return
				}
				done <- err
			}()
			<-entered
			mutate(d)
			before := cloneForTest(d)
			close(release)
			if err := <-done; err != ErrStateChanged {
				t.Fatal(err)
			}
			assertSchedulingUnchanged(t, d, before)
		})
	}
}

func TestDynamicQuantumConcurrentTakesAndConservation(t *testing.T) {
	d := NewDWRR[int](1)
	mustEnqueue(t, d, 0, []Item[int]{{1, 1}, {2, 1}})
	var entered sync.WaitGroup
	entered.Add(2)
	release := make(chan struct{})
	_ = d.SetQuantum(0, func() uint64 { entered.Done(); <-release; return 3 })
	errors := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, err := d.Take(1); errors <- err }()
	}
	entered.Wait()
	close(release)
	a, b := <-errors, <-errors
	if !((a == nil && b == ErrStateChanged) || (b == nil && a == ErrStateChanged)) || d.total != 1 || d.deficits[0] != 2 || !d.active {
		t.Fatal(a, b, d)
	}
	const count = 1000
	d = NewDWRR[int](1)
	items := make([]Item[int], count)
	for i := range items {
		items[i] = Item[int]{i, 1}
	}
	mustEnqueue(t, d, 0, items)
	_ = d.SetQuantum(0, func() uint64 { return 3 })
	results := make(chan []Scheduled[int], 4)
	for worker := 0; worker < 4; worker++ {
		go func() {
			var all []Scheduled[int]
			for {
				batch, err := d.Take(7)
				if err == ErrStateChanged {
					continue // Explicit application retry, never an internal retry.
				}
				if err != nil {
					t.Error(err)
					break
				}
				if len(batch) == 0 {
					break
				}
				all = append(all, batch...)
			}
			results <- all
		}()
	}
	seen := make([]bool, count)
	for worker := 0; worker < 4; worker++ {
		for _, item := range <-results {
			if item.Value < 0 || item.Value >= count || seen[item.Value] {
				t.Fatal("duplicate or invalid work", item)
			}
			seen[item.Value] = true
		}
	}
	for i, ok := range seen {
		if !ok {
			t.Fatal("lost item", i)
		}
	}
}

func TestGenerationDoesNotWrap(t *testing.T) {
	d := NewDWRR[int](1)
	mustEnqueue(t, d, 0, []Item[int]{{1, 1}})
	d.generation = ^uint64(0)
	before := cloneForTest(d)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("generation exhaustion must panic before mutation")
			}
		}()
		d.Take(1)
	}()
	assertSchedulingUnchanged(t, d, before)
}

func TestDynamicQuantumChangingGrantsDoNotStarve(t *testing.T) {
	d := NewDWRR[int](2)
	calls := 0
	_ = d.SetQuantum(0, func() uint64 { calls++; return 1 + uint64(calls%2) })
	mustEnqueue(t, d, 0, []Item[int]{{7, 40}})
	items := make([]Item[int], 100)
	for i := range items {
		items[i] = Item[int]{9, 1}
	}
	mustEnqueue(t, d, 1, items)
	for call := 0; call < 100; call++ {
		got := takeN(d, 1)
		if got[0].Queue == 0 {
			if got[0].Value != 7 || got[0].Cost != 40 || calls < 20 || calls > 40 {
				t.Fatal("changing quanta lost fixed-cost credit", got, calls)
			}
			return
		}
	}
	t.Fatal("changing per-grant source starved its queue")
}

func TestDynamicQuantumExtremeGrants(t *testing.T) {
	d := NewDWRR[int](2)
	calls := 0
	_ = d.SetQuantum(0, func() uint64 { calls++; return MaxQuantum - 1 })
	mustEnqueue(t, d, 0, []Item[int]{{1, MaxCost}, {2, 1}, {3, 1}})
	mustEnqueue(t, d, 1, []Item[int]{{9, 1}})
	if got := takeN(d, 1); !reflect.DeepEqual(got, []Scheduled[int]{{1, 9, 1}}) || d.deficits[0] != MaxCost-1 || calls != 1 {
		t.Fatal(got, d.deficits, calls)
	}
	d.RemoveHead(0)
	_ = d.SetQuantum(0, func() uint64 { return MaxQuantum })
	if got := takeN(d, 1); !reflect.DeepEqual(got, []Scheduled[int]{{0, 2, 1}}) || d.deficits[0] != ^uint64(0)-3 || !d.active {
		t.Fatal("dynamic grant lost exact peak credit", got, d.deficits)
	}
}
