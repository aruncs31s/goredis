package goredis

import "encoding/json"

// Codec serializes and deserializes cache values.
type Codec interface {
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
}

type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (JSONCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
