package models

import (
	"testing"
	"time"
)

func TestDuration_Window(t *testing.T) {
	cases := []struct {
		d    Duration
		want time.Duration
	}{
		{Duration5m, 5 * time.Minute},
		{Duration15m, 15 * time.Minute},
		{Duration("bogus"), 0},
	}
	for _, c := range cases {
		if got := c.d.Window(); got != c.want {
			t.Errorf("Duration(%q).Window() = %v, want %v", c.d, got, c.want)
		}
	}
}
