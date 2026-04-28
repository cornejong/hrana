package hrana

import "encoding/json"

// Codec abstracts message encoding so JSON and Protobuf share the same pipeline.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
	// ContentType returns the MIME type for HTTP Content-Type headers.
	ContentType() string
}

// JSONCodec implements Codec using encoding/json.
type JSONCodec struct{}

func (JSONCodec) Encode(v any) ([]byte, error)    { return json.Marshal(v) }
func (JSONCodec) Decode(data []byte, v any) error { return json.Unmarshal(data, v) }
func (JSONCodec) ContentType() string             { return "application/json" }

// DefaultCodec is the JSON codec used by all current protocol versions.
var DefaultCodec Codec = JSONCodec{}
