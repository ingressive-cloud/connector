package awsv4_test

import (
	"net/http"
	"testing"

	"github.com/ingressive-cloud/connector/awsv4"
)

func TestSignRequest_SetsAuthorizationHeader(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://api.example.com/gateways/abc/ws", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	if err := awsv4.SignRequest(req, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); err != nil {
		t.Fatalf("SignRequest: %v", err)
	}

	auth := req.Header.Get("Authorization")
	if auth == "" {
		t.Fatal("expected Authorization header to be set")
	}
	if req.Header.Get("X-Amz-Date") == "" {
		t.Fatal("expected X-Amz-Date header to be set")
	}
	if !containsStr(auth, "AWS4-HMAC-SHA256") {
		t.Errorf("Authorization missing AWS4-HMAC-SHA256: %q", auth)
	}
	if !containsStr(auth, "global/api") {
		t.Errorf("Authorization missing region/service: %q", auth)
	}
}

func TestSignRequest_DifferentKeysProduceDifferentSignatures(t *testing.T) {
	makeReq := func() *http.Request {
		req, _ := http.NewRequest(http.MethodGet, "https://api.example.com/gateways/abc/ws", nil)
		return req
	}

	req1, req2 := makeReq(), makeReq()
	_ = awsv4.SignRequest(req1, "KEY1", "SECRET1")
	_ = awsv4.SignRequest(req2, "KEY2", "SECRET2")

	if req1.Header.Get("Authorization") == req2.Header.Get("Authorization") {
		t.Error("different keys should produce different Authorization headers")
	}
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
