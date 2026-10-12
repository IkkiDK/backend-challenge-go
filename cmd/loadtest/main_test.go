package main

import (
	"testing"
	"time"
)

func TestPercentileUsesTheNearestRank(t *testing.T) {
	samples := make([]time.Duration, 100)
	for i := range samples {
		samples[i] = time.Duration(i+1) * time.Millisecond
	}
	cases := []struct {
		p    float64
		want time.Duration
	}{
		{0.50, 50 * time.Millisecond},
		{0.95, 95 * time.Millisecond},
		{0.99, 99 * time.Millisecond},
		{1, 100 * time.Millisecond},
		{0.001, time.Millisecond},
	}
	for _, c := range cases {
		if got := percentile(samples, c.p); got != c.want {
			t.Errorf("percentile(%v) = %v, want %v", c.p, got, c.want)
		}
	}
	if got := percentile(nil, 0.99); got != 0 {
		t.Errorf("percentile of no samples = %v, want 0", got)
	}
	if got := percentile([]time.Duration{7 * time.Millisecond}, 0.99); got != 7*time.Millisecond {
		t.Errorf("percentile of one sample = %v, want 7ms", got)
	}
	if got := percentile(samples[:10], 0.95); got != 10*time.Millisecond {
		t.Errorf("p95 of 10 samples = %v, want the 10th (rank 9.5 rounds up)", got)
	}
}

func TestMetricsSumMatchesNameAndEveryLabel(t *testing.T) {
	m := metrics{
		`wallet_sqs_messages_total{outcome="processed"}`:       3,
		`wallet_sqs_messages_total{outcome="released"}`:        2,
		`wallet_sqs_messages_total_extra{outcome="processed"}`: 100,
		`wallet_lock_wait_seconds_count`:                       5,
	}
	if got := m.sum("wallet_sqs_messages_total"); got != 5 {
		t.Errorf("sum over all labels = %v, want 5", got)
	}
	if got := m.sum("wallet_sqs_messages_total", `outcome="processed"`); got != 3 {
		t.Errorf("sum with a label = %v, want 3", got)
	}
	if got := m.sum("wallet_lock_wait_seconds_count"); got != 5 {
		t.Errorf("sum of an unlabelled series = %v, want 5", got)
	}
	if got := m.minus(metrics{`wallet_lock_wait_seconds_count`: 2}).sum("wallet_lock_wait_seconds_count"); got != 3 {
		t.Errorf("delta = %v, want 3", got)
	}
}
