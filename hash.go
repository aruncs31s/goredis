package goredis

import (
	"crypto/sha256"
	"fmt"
)

// HashRequest generates a deterministic hex hash from a request struct.
func (c *Client) HashRequest(req any) (string, error) {
	reqBytes, err := c.codec.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request for hashing: %w", err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(reqBytes)), nil
}
