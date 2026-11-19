package dwrr_test

import (
	"math/big"
	"testing"
	"time"

	"github.com/version2beta/dwrr"
)

func TestDeadlineQuantum(t *testing.T) {
	const maxTime = time.Duration(1<<63 - 1)
	for _, tc := range []struct {
		name                string
		now, deadline, ramp time.Duration
		low, high, want     uint64
	}{
		{"far", 0, 100, 10, 2, 10, 2},
		{"rampStart", 90, 100, 10, 2, 10, 2},
		{"middle", 95, 100, 10, 2, 10, 6},
		{"floor", 91, 100, 10, 2, 10, 2},
		{"near", 99, 100, 10, 2, 10, 9},
		{"at", 100, 100, 10, 2, 10, 10},
		{"past", 101, 100, 10, 2, 10, 10},
		{"deadlineZero", 0, 0, 1, 1, dwrr.MaxQuantum, dwrr.MaxQuantum},
		{"flat", 5, 10, 10, 7, 7, 7},
		{"maxTimesFar", 0, maxTime, 1, 1, dwrr.MaxQuantum, 1},
		{"maxTimesMiddle", maxTime / 2, maxTime, maxTime, 1, dwrr.MaxQuantum, uint64(maxTime / 2)},
		{"maxTimesNear", maxTime - 1, maxTime, maxTime, 1, dwrr.MaxQuantum, dwrr.MaxQuantum - 1},
		{"maxAt", maxTime, maxTime, maxTime, 1, dwrr.MaxQuantum, dwrr.MaxQuantum},
		{"maxPast", maxTime, 0, maxTime, 1, dwrr.MaxQuantum, dwrr.MaxQuantum},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := deadlineQuantum(tc.now, tc.deadline, tc.ramp, tc.low, tc.high)
			if err != nil || got != tc.want {
				t.Fatalf("got (%d,%v), want %d", got, err, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		now, deadline, ramp time.Duration
		low, high           uint64
	}{
		{-1, 10, 1, 1, 2}, {0, -1, 1, 1, 2}, {0, 10, 0, 1, 2}, {0, 10, -1, 1, 2},
		{0, 10, 1, 0, 2}, {0, 10, 1, 2, 1}, {0, 10, 1, 1, dwrr.MaxQuantum + 1},
		// Clamp branches must not bypass validation.
		{10, 0, 0, 1, 2}, {10, 0, 1, 0, 2},
	} {
		if got, err := deadlineQuantum(tc.now, tc.deadline, tc.ramp, tc.low, tc.high); got != 0 || err == nil {
			t.Fatalf("invalid policy %+v: (%d,%v)", tc, got, err)
		}
	}
}

func TestDeadlineQuantumExactFullRange(t *testing.T) {
	const ramp = time.Duration(1<<63 - 1)
	for _, low := range []uint64{1, 2, dwrr.MaxQuantum - 1, dwrr.MaxQuantum} {
		previous := low
		for _, now := range []time.Duration{0, 1, 2, ramp / 3, ramp / 2, ramp - 2, ramp - 1, ramp} {
			// Independent arbitrary-precision interpolation checks Mul64/Div64
			// and rounding, not just floating-point approximations of the ramp.
			want := new(big.Int).SetUint64(dwrr.MaxQuantum - low)
			want.Mul(want, big.NewInt(int64(now)))
			want.Div(want, big.NewInt(int64(ramp)))
			want.Add(want, new(big.Int).SetUint64(low))
			got, err := deadlineQuantum(now, ramp, ramp, low, dwrr.MaxQuantum)
			if err != nil || got != want.Uint64() || got < previous || got > dwrr.MaxQuantum {
				t.Fatalf("low=%d now=%d: got (%d,%v), want %s, previous=%d", low, now, got, err, want, previous)
			}
			previous = got
		}
	}
}
