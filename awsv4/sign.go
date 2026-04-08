package awsv4

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	// emptyBodySHA256 is the SHA-256 hash of an empty request body.
	emptyBodySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	awsRegion  = "global"
	awsService = "api"
)

// SignRequest signs an HTTP request using AWS Signature Version 4.
// It mutates req by adding Authorization and X-Amz-Date headers.
// Used to authenticate the WebSocket upgrade handshake.
func SignRequest(req *http.Request, keyID, keySecret string) error {
	creds := aws.Credentials{
		AccessKeyID:     keyID,
		SecretAccessKey: keySecret,
	}
	signer := v4.NewSigner()
	return signer.SignHTTP(context.Background(), creds, req, emptyBodySHA256, awsService, awsRegion, time.Now().UTC())
}
