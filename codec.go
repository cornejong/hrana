package hrana

import (
	"encoding/json"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

// Codec abstracts message encoding so JSON and Msgpack share the same pipeline.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
	DecodeReader(r io.Reader, v any) error
	EncodeWriter(w io.Writer, v any) error
	// ContentType returns the MIME type for HTTP Content-Type headers.
	ContentType() string
}

// JSONCodec implements Codec using encoding/json.
type JSONCodec struct{}

func (JSONCodec) Encode(v any) ([]byte, error)          { return json.Marshal(v) }
func (JSONCodec) Decode(data []byte, v any) error       { return json.Unmarshal(data, v) }
func (JSONCodec) DecodeReader(r io.Reader, v any) error { return json.NewDecoder(r).Decode(v) }
func (JSONCodec) EncodeWriter(w io.Writer, v any) error { return json.NewEncoder(w).Encode(v) }
func (JSONCodec) ContentType() string                   { return "application/json" }

// MsgpackCodec implements Codec using github.com/vmihailenco/msgpack/v5.
type MsgpackCodec struct{}

func (MsgpackCodec) Encode(v any) ([]byte, error)          { return msgpack.Marshal(v) }
func (MsgpackCodec) Decode(data []byte, v any) error       { return msgpack.Unmarshal(data, v) }
func (MsgpackCodec) DecodeReader(r io.Reader, v any) error { return msgpack.NewDecoder(r).Decode(v) }
func (MsgpackCodec) EncodeWriter(w io.Writer, v any) error { return msgpack.NewEncoder(w).Encode(v) }
func (MsgpackCodec) ContentType() string                   { return "application/x-msgpack" }

// DefaultCodec is the JSON codec used by all current protocol versions.
var DefaultCodec Codec = JSONCodec{}
