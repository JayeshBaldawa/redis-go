package main

import (
	"testing"
)

func TestHandleCommand(t *testing.T) {
	tests := []struct {
		name           string
		orginalCommand []byte
		expectedResp   string
	}{
		{
			name:           "Ping Command",
			orginalCommand: []byte("PING\r\n"),
			expectedResp:   "+PONG\r\n",
		},
		{
			name:           "Array Command",
			orginalCommand: []byte("*2\r\n$4\r\necho\r\n$3\r\nhey\r\n"),
			expectedResp:   "$3\r\nhey\r\n",
		},
		{
			name:           "Ping Command",
			orginalCommand: []byte("*1\r\n$4\r\nping\r\n"),
			expectedResp:   "+PONG\r\n",
		},
		{
			name:           "Array Command",
			orginalCommand: []byte("*1\r\n$4\r\nping\r\nG"),
			expectedResp:   "+PONG\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := handleCommand(string(tt.orginalCommand))
			if resp != tt.expectedResp {
				t.Errorf("unexpected response, got: %s, want: %s", resp, tt.expectedResp)
			}
		})
	}
}
