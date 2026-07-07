package zitihost

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/openziti/edge-api/rest_client_api_client/current_identity"
	"github.com/openziti/edge-api/rest_util"
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

// TestIsAuthRejection — WatchHealth must only self-terminate on a real
// auth-level rejection (401/403 from a reachable controller), never on a
// transient/unreachable-controller error. This guards that classification,
// which is the crux of the fleet-crashloop fix.
func TestIsAuthRejection(t *testing.T) {
	// Typed 401 response (edge-api's Code() method), as returned by
	// GetCurrentIdentity — and again wrapped by rest_util.WrapErr, which is how
	// the SDK actually surfaces it.
	unauthorized := current_identity.NewGetCurrentIdentityUnauthorized()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"typed 401 response", unauthorized, true},
		{"typed 401 wrapped by WrapErr", rest_util.WrapErr(unauthorized), true},
		{"typed 401 fmt-wrapped", fmt.Errorf("health: %w", unauthorized), true},
		{"runtime APIError 403", runtime.NewAPIError("op", "forbidden", 403), true},
		{"runtime APIError 503", runtime.NewAPIError("op", "unavailable", 503), false},
		{"network dial error", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, false},
		{"plain error", errors.New("some transient failure"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAuthRejection(tt.err); got != tt.want {
				t.Errorf("isAuthRejection(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
