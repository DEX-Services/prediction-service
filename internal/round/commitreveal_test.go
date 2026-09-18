package round

import (
	"testing"

	"github.com/dex/prediction-service/internal/models"
)

func TestOffsetRangeBps(t *testing.T) {
	cases := []struct {
		d      models.Duration
		lo, hi int64
	}{
		{models.Duration5m, 5, 15},
		{models.Duration15m, 15, 40},
	}
	for _, c := range cases {
		lo, hi := offsetRangeBps(c.d)
		if lo != c.lo || hi != c.hi {
			t.Errorf("offsetRangeBps(%v) = (%d, %d), want (%d, %d)", c.d, lo, hi, c.lo, c.hi)
		}
	}
}

func TestDrawOffsetBps_WithinRangeAndSignedBothWays(t *testing.T) {
	lo, hi := offsetRangeBps(models.Duration5m)
	sawPositive, sawNegative := false, false
	for i := 0; i < 500; i++ {
		v, err := drawOffsetBps(models.Duration5m)
		if err != nil {
			t.Fatalf("drawOffsetBps: %v", err)
		}
		mag := v
		if mag < 0 {
			mag = -mag
			sawNegative = true
		} else {
			sawPositive = true
		}
		if mag < lo || mag > hi {
			t.Fatalf("drawOffsetBps magnitude %d outside range [%d, %d]", mag, lo, hi)
		}
	}
	// Over 500 draws with a fair coin, seeing only one sign is
	// astronomically unlikely (~2^-500) — this is a correctness check on
	// the sign draw, not a statistical/flaky test.
	if !sawPositive || !sawNegative {
		t.Fatalf("expected both signs across 500 draws, got positive=%v negative=%v", sawPositive, sawNegative)
	}
}

func TestCommitHash_DeterministicForSameInputs(t *testing.T) {
	h1 := commitHash(42, "somesalt")
	h2 := commitHash(42, "somesalt")
	if h1 != h2 {
		t.Fatalf("commitHash not deterministic: %q != %q", h1, h2)
	}
}

func TestCommitHash_DifferentInputsDifferentHash(t *testing.T) {
	h1 := commitHash(42, "salt-a")
	h2 := commitHash(-42, "salt-a")
	h3 := commitHash(42, "salt-b")
	if h1 == h2 {
		t.Fatal("commitHash(42, salt) == commitHash(-42, salt), sign not distinguished")
	}
	if h1 == h3 {
		t.Fatal("commitHash(offset, salt-a) == commitHash(offset, salt-b), salt not distinguished")
	}
}

func TestNewCommitment_VerifiesAndIsFresh(t *testing.T) {
	offset1, salt1, hash1, err := NewCommitment(models.Duration5m)
	if err != nil {
		t.Fatalf("NewCommitment: %v", err)
	}
	if !VerifyCommitment(offset1, salt1, hash1) {
		t.Fatal("VerifyCommitment rejected a commitment NewCommitment just produced")
	}

	offset2, salt2, hash2, err := NewCommitment(models.Duration5m)
	if err != nil {
		t.Fatalf("NewCommitment (2nd): %v", err)
	}
	if salt1 == salt2 {
		t.Fatal("two NewCommitment calls produced the same salt — randomSalt is not actually random")
	}
	if hash1 == hash2 && offset1 == offset2 {
		// Extremely unlikely coincidence given random salts differ; only a
		// real bug (e.g. salt not mixed into the hash) would make this
		// reproducibly true across runs.
		t.Fatal("two commitments with different salts produced the same hash")
	}
}

func TestVerifyCommitment_RejectsTamperedOffset(t *testing.T) {
	offset, salt, hash, err := NewCommitment(models.Duration15m)
	if err != nil {
		t.Fatalf("NewCommitment: %v", err)
	}
	if VerifyCommitment(offset+1, salt, hash) {
		t.Fatal("VerifyCommitment accepted a tampered offset — commit-reveal integrity is broken")
	}
	if VerifyCommitment(offset, salt+"x", hash) {
		t.Fatal("VerifyCommitment accepted a tampered salt — commit-reveal integrity is broken")
	}
}
