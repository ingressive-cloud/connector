package zitihost

import (
	"strings"
	"testing"
)

// TestEnroll_EmptyJWT — Enroll must refuse an empty token rather than reach
// out to a controller. Cheap to test, catches a common caller mistake.
func TestEnroll_EmptyJWT(t *testing.T) {
	_, err := Enroll("")
	if err == nil {
		t.Fatal("expected error for empty JWT, got nil")
	}
	if !strings.Contains(err.Error(), "jwt is empty") {
		t.Errorf("expected 'jwt is empty' in error, got %q", err.Error())
	}
}

// TestEnroll_GarbageJWT — ParseToken should reject something that isn't a
// JWT at all. Verifies the error path surfaces ParseToken's error rather than
// panicking somewhere downstream.
func TestEnroll_GarbageJWT(t *testing.T) {
	_, err := Enroll("not-a-jwt")
	if err == nil {
		t.Fatal("expected error for malformed JWT, got nil")
	}
	if !strings.Contains(err.Error(), "parse enrollment JWT") {
		t.Errorf("expected ParseToken error, got %q", err.Error())
	}
}
