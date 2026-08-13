package hranago

import (
	"encoding/json"
	"errors"

	"github.com/vmihailenco/msgpack/v5"
)

// Codec defines the Marshalling interface for Hrana WS payloads (JSON or Msgpack).
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
	IsBinary() bool // Determines if payloads should be sent as ArrayBuffer vs String
}

// ---------------------------------------------------------------------
// RawMessage
// ---------------------------------------------------------------------

// RawMessage holds raw encoded data and delays its decoding.
// It implements both json.Unmarshaler and msgpack.CustomUnmarshaler
// so that the nested payload structures (like response_ok) work with both codecs.
type RawMessage []byte

// MarshalJSON returns m as the JSON encoding of m.
func (m RawMessage) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}

	return m, nil
}

// UnmarshalJSON sets *m to a copy of data.
func (m *RawMessage) UnmarshalJSON(data []byte) error {
	if m == nil {
		return errors.New("hrana: UnmarshalJSON on nil pointer")
	}

	*m = append((*m)[0:0], data...)
	return nil
}

// EncodeMsgpack writes the raw msgpack bytes directly.
func (m RawMessage) EncodeMsgpack(enc *msgpack.Encoder) error {
	if m == nil {
		return enc.EncodeNil()
	}

	_, err := enc.Writer().Write(m)
	return err
}

// DecodeMsgpack reads the raw msgpack bytes into *m.
func (m *RawMessage) DecodeMsgpack(dec *msgpack.Decoder) error {
	if m == nil {
		return errors.New("hrana: DecodeMsgpack on nil pointer")
	}

	b, err := dec.DecodeRaw()
	if err != nil {
		return err
	}
	*m = append((*m)[0:0], b...)
	return nil
}

// ---------------------------------------------------------------------
// JSON Codec
// ---------------------------------------------------------------------

// JSONCodec provides the default encoding/json implementation.
type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (JSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (JSONCodec) IsBinary() bool                     { return false }

// ---------------------------------------------------------------------
// MsgPack Codec
// ---------------------------------------------------------------------

type MsgpackCodec struct{}

func (MsgpackCodec) Marshal(v any) ([]byte, error)      { return msgpack.Marshal(v) }
func (MsgpackCodec) Unmarshal(data []byte, v any) error { return msgpack.Unmarshal(data, v) }
func (MsgpackCodec) IsBinary() bool                     { return true }
