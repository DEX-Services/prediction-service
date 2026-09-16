package round

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/dex/prediction-service/internal/models"
)

// offsetRangeBps returns the (min, max) absolute basis-point offset range
// for a duration, per the user's explicit requirement: the random offset is
// never a fixed percentage (that would let a user predict the next round's
// target), only a fresh draw within a tight per-duration range each round.
//
//	5m:  0.05%-0.15%  -> 5-15 bps
//	15m: 0.15%-0.40%  -> 15-40 bps
func offsetRangeBps(d models.Duration) (min, max int64) {
	switch d {
	case models.Duration5m:
		return 5, 15
	case models.Duration15m:
		return 15, 40
	}
	return 5, 15
}

// drawOffsetBps cryptographically draws a fresh signed offset in basis
// points for a round, uniformly at random within the duration's range and
// with a uniformly random sign.
func drawOffsetBps(d models.Duration) (int64, error) {
	lo, hi := offsetRangeBps(d)
	span := hi - lo + 1
	n, err := rand.Int(rand.Reader, big.NewInt(span))
	if err != nil {
		return 0, err
	}
	magnitude := lo + n.Int64()

	signBit, err := rand.Int(rand.Reader, big.NewInt(2))
	if err != nil {
		return 0, err
	}
	if signBit.Int64() == 0 {
		magnitude = -magnitude
	}
	return magnitude, nil
}

// randomSalt returns a fresh hex-encoded random salt to mix into the commit
// hash, so the hash can't be brute-forced from the small offset space alone.
func randomSalt() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// commitHash computes sha256(offsetBps || salt) as the publishable commitment.
func commitHash(offsetBps int64, salt string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%s", offsetBps, salt)))
	return hex.EncodeToString(sum[:])
}

// NewCommitment draws a fresh random offset+salt for a round and returns the
// commit hash to publish immediately, plus the offset/salt to reveal later
// (kept secret by the caller until reveal time).
func NewCommitment(d models.Duration) (offsetBps int64, salt string, hash string, err error) {
	offsetBps, err = drawOffsetBps(d)
	if err != nil {
		return 0, "", "", err
	}
	salt, err = randomSalt()
	if err != nil {
		return 0, "", "", err
	}
	hash = commitHash(offsetBps, salt)
	return offsetBps, salt, hash, nil
}

// VerifyCommitment checks that offsetBps+salt actually hash to the
// previously published commitHash — anyone (including an external auditor)
// can run this to confirm the revealed target wasn't tampered with.
func VerifyCommitment(offsetBps int64, salt, hash string) bool {
	return commitHash(offsetBps, salt) == hash
}
