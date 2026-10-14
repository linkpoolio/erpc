package common

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedactedRequestURL(t *testing.T) {
	cases := map[string]string{
		"/main/evm/1":                           "/main/evm/1",
		"/main/evm/1?foo=bar":                   "/main/evm/1?foo=bar",
		"/main/evm/1?secret=s3cret":             "/main/evm/1?secret=REDACTED",
		"/main/evm/1?token=t0k&foo=bar":         "/main/evm/1?foo=bar&token=REDACTED",
		"/main/evm/1?jwt=a.b.c":                 "/main/evm/1?jwt=REDACTED",
		"http://host:4000/main/evm/1?secret=x":  "http://host:4000/main/evm/1?secret=REDACTED",
		"/main/evm/1?secret=a&secret=b":         "/main/evm/1?secret=REDACTED&secret=REDACTED",
		"/main/evm/1?signature=0x1&message=abc": "/main/evm/1?signature=0x1&message=abc",
	}
	for in, want := range cases {
		u, err := url.Parse(in)
		require.NoError(t, err)
		assert.Equal(t, want, RedactedRequestURL(u), in)
	}
	assert.Equal(t, "", RedactedRequestURL(nil))
}
