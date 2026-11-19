package dwrr_test

import (
	"errors"
	"fmt"
	"math/bits"
	"time"

	"github.com/version2beta/dwrr"
)

// deadlineQuantum is application policy, not part of dwrr. now and deadline
// are nonnegative elapsed timestamps from the same monotonic origin, not Unix
// times or time-of-day readings. ramp must be positive; 1 <= low <= high <=
// MaxQuantum. Invalid inputs return zero and an error, never a usable quantum.
// The linear ramp is rounded down and clamped to [low, high].
func deadlineQuantum(now, deadline, ramp time.Duration, low, high uint64) (uint64, error) {
	if now < 0 || deadline < 0 || ramp <= 0 || low == 0 || low > high || high > dwrr.MaxQuantum {
		return 0, errors.New("invalid deadline quantum policy")
	}
	if now >= deadline {
		return high, nil
	}
	// Ordering plus nonnegative timestamps makes subtraction safe even at the
	// time.Duration limit. Do not compute deadline-ramp or now+ramp.
	remaining := deadline - now
	if remaining >= ramp {
		return low, nil
	}
	// Exact wide multiplication avoids overflow for full-range quanta and
	// durations. elapsed < ramp, so the quotient fits and Div64 cannot overflow.
	hi, lo := bits.Mul64(high-low, uint64(ramp-remaining))
	increment, _ := bits.Div64(hi, lo, uint64(ramp))
	return low + increment, nil
}

// Each application queue owns its policy and supplied monotonic timestamp.
// No scheduler clock, background task, or media-specific policy is needed.
type deadlineStream struct {
	now, deadline, ramp time.Duration
	low, high           uint64
}

func (s *deadlineStream) GetQuantum() uint64 {
	quantum, err := deadlineQuantum(s.now, s.deadline, s.ramp, s.low, s.high)
	if err != nil {
		panic(err) // This example treats invalid application configuration as fatal.
	}
	return quantum
}

func ExampleDWRR_SetQuantum() {
	streams := []*deadlineStream{
		{deadline: 10 * time.Second, ramp: 10 * time.Second, low: 1, high: 5},
		{deadline: 20 * time.Second, ramp: 10 * time.Second, low: 1, high: 5},
	}
	s := dwrr.NewDWRR[string](2)
	for q, stream := range streams {
		if err := s.SetQuantum(uint(q), stream.GetQuantum); err != nil {
			panic(err)
		}
	}
	if err := s.Enqueue(0, []dwrr.Item[string]{{Value: "frame-a", Cost: 2}, {Value: "frame-b", Cost: 2}}); err != nil {
		panic(err)
	}
	if err := s.Enqueue(1, []dwrr.Item[string]{{Value: "frame-x", Cost: 2}}); err != nil {
		panic(err)
	}
	for _, now := range []time.Duration{0, 5 * time.Second, 10 * time.Second} {
		for _, stream := range streams {
			stream.now = now // Single application goroutine; no actual time passes.
		}
		// GetQuantum runs on EACH new grant, including multiple grants in a
		// batch. Fixed item costs do not change. Allocation is not a deadline
		// guarantee: downstream execution remains the application's job.
		batch, err := s.Take(1)
		if err != nil {
			panic(err)
		}
		fmt.Println(now, batch[0].Value)
	}
	// Output:
	// 0s frame-a
	// 5s frame-x
	// 10s frame-b
}
