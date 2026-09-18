package auth

import (
	"testing"
	"time"
)

func TestIssueThenVerify_RoundTrips(t *testing.T) {
	issuer := NewJWTIssuer("test-secret", time.Hour)
	token, expiresAt, err := issuer.Issue("user-1", "0xabc")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if expiresAt.Before(time.Now()) {
		t.Fatal("Issue returned an already-expired expiresAt")
	}

	claims, err := issuer.Verify(token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.UserID != "user-1" {
		t.Errorf("UserID = %q, want user-1", claims.UserID)
	}
	if claims.WalletAddress != "0xabc" {
		t.Errorf("WalletAddress = %q, want 0xabc", claims.WalletAddress)
	}
}

func TestVerify_RejectsTokenSignedWithDifferentSecret(t *testing.T) {
	issuerA := NewJWTIssuer("secret-a", time.Hour)
	issuerB := NewJWTIssuer("secret-b", time.Hour)

	token, _, err := issuerA.Issue("user-1", "0xabc")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := issuerB.Verify(token); err == nil {
		t.Fatal("Verify accepted a token signed with a different secret")
	}
}

func TestVerify_RejectsExpiredToken(t *testing.T) {
	// A negative TTL issues an already-expired token.
	issuer := NewJWTIssuer("test-secret", -time.Hour)
	token, _, err := issuer.Issue("user-1", "0xabc")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := issuer.Verify(token); err == nil {
		t.Fatal("Verify accepted an expired token")
	}
}

func TestVerify_RejectsMalformedToken(t *testing.T) {
	issuer := NewJWTIssuer("test-secret", time.Hour)
	if _, err := issuer.Verify("not-a-real-jwt"); err == nil {
		t.Fatal("Verify accepted a malformed token string")
	}
}

func TestVerify_RejectsEmptyToken(t *testing.T) {
	issuer := NewJWTIssuer("test-secret", time.Hour)
	if _, err := issuer.Verify(""); err == nil {
		t.Fatal("Verify accepted an empty token string")
	}
}
