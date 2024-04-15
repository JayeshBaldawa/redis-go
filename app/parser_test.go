package main

import (
	"testing"
	"time"
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
		{
			name:           "Array Command",
			orginalCommand: []byte("*3\r\n$3\r\nset\r\n$10\r\nstrawberry\r\n$9\r\nblueberry\r\n"),
			expectedResp:   "+OK\r\n",
		}, {
			name:           "Array Command",
			orginalCommand: []byte("*2\r\n$3\r\nget\r\n$10\r\nstrawberry\r\n"),
			expectedResp:   "$9\r\nblueberry\r\n",
		}, {
			name:           "Array Command",
			orginalCommand: []byte("*5\r\n$3\r\nset\r\n$9\r\nblueberry\r\n$9\r\nraspberry\r\n$2\r\npx\r\n$3\r\n100\r\n"),
			expectedResp:   "+OK\r\n",
		}, {
			name:           "Timeout Command",
			orginalCommand: []byte("*2\r\n$3\r\nget\r\n$9\r\nblueberry\r\n"),
			expectedResp:   "$-1\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "Timeout Command" {
				time.Sleep(1 * time.Second)
			}
			resp := handleCommand(string(tt.orginalCommand))
			if resp != tt.expectedResp {
				t.Errorf("unexpected response, got: %s, want: %s", resp, tt.expectedResp)
			}
		})
	}
}
