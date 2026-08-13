package hranago

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/vmihailenco/msgpack/v5"
)

// ─── Value ───────────────────────────────────────────────────────────────────

// Value represents a single SQLite value in the Hrana wire format.
type Value struct {
	typ     string
	integer int64
	float   float64
	text    string
	blob    []byte
}

func nullValue() Value           { return Value{typ: "null"} }
func integerValue(i int64) Value { return Value{typ: "integer", integer: i} }
func floatValue(f float64) Value { return Value{typ: "float", float: f} }
func textValue(s string) Value   { return Value{typ: "text", text: s} }
func blobValue(b []byte) Value   { return Value{typ: "blob", blob: b} }

func (v Value) MarshalJSON() ([]byte, error) {
	switch v.typ {
	case "null", "":
		return []byte(`{"type":"null"}`), nil
	case "integer":
		s := strconv.FormatInt(v.integer, 10)
		return json.Marshal(map[string]string{"type": "integer", "value": s})
	case "float":
		return json.Marshal(map[string]any{"type": "float", "value": v.float})
	case "text":
		return json.Marshal(map[string]string{"type": "text", "value": v.text})
	case "blob":
		return json.Marshal(map[string]string{"type": "blob", "base64": base64.StdEncoding.EncodeToString(v.blob)})
	default:
		return nil, fmt.Errorf("hrana: unknown value type %q", v.typ)
	}
}

func (v *Value) UnmarshalJSON(data []byte) error {
	var raw struct {
		Type   string          `json:"type"`
		Value  json.RawMessage `json:"value"`
		Base64 string          `json:"base64"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	v.typ = raw.Type
	switch raw.Type {
	case "null":
		// nothing to set
	case "integer":
		var s string
		if err := json.Unmarshal(raw.Value, &s); err != nil {
			return fmt.Errorf("hrana: integer value must be a string: %w", err)
		}
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("hrana: invalid integer value %q: %w", s, err)
		}
		v.integer = i
	case "float":
		var f float64
		if err := json.Unmarshal(raw.Value, &f); err != nil {
			return fmt.Errorf("hrana: float value must be a number: %w", err)
		}
		v.float = f
	case "text":
		var s string
		if err := json.Unmarshal(raw.Value, &s); err != nil {
			return fmt.Errorf("hrana: text value must be a string: %w", err)
		}
		v.text = s
	case "blob":
		b, err := base64.StdEncoding.DecodeString(raw.Base64)
		if err != nil {
			return fmt.Errorf("hrana: invalid base64 blob: %w", err)
		}
		v.blob = b
	default:
		return fmt.Errorf("hrana: unknown value type %q", raw.Type)
	}
	return nil
}

func (v Value) MarshalMsgpack() ([]byte, error) {
	switch v.typ {
	case "null", "":
		return msgpack.Marshal(map[string]string{"type": "null"})
	case "integer":
		return msgpack.Marshal(map[string]string{
			"type":  "integer",
			"value": strconv.FormatInt(v.integer, 10),
		})
	case "float":
		return msgpack.Marshal(map[string]any{"type": "float", "value": v.float})
	case "text":
		return msgpack.Marshal(map[string]string{"type": "text", "value": v.text})
	case "blob":
		return msgpack.Marshal(map[string]string{
			"type":   "blob",
			"base64": base64.StdEncoding.EncodeToString(v.blob),
		})
	default:
		return nil, fmt.Errorf("hrana: unknown value type %q", v.typ)
	}
}

func (v *Value) UnmarshalMsgpack(data []byte) error {
	var raw struct {
		Type   string     `msgpack:"type"`
		Value  RawMessage `msgpack:"value"`
		Base64 string     `msgpack:"base64"`
	}
	if err := msgpack.Unmarshal(data, &raw); err != nil {
		return err
	}
	v.typ = raw.Type
	switch raw.Type {
	case "null":
		// nothing
	case "integer":
		var s string
		if err := msgpack.Unmarshal(raw.Value, &s); err != nil {
			return fmt.Errorf("hrana: integer value: %w", err)
		}
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("hrana: integer value %q: %w", s, err)
		}
		v.integer = i
	case "float":
		var f float64
		if err := msgpack.Unmarshal(raw.Value, &f); err != nil {
			var f32 float32
			if err2 := msgpack.Unmarshal(raw.Value, &f32); err2 != nil {
				return fmt.Errorf("hrana: float value: %w", err)
			}
			v.float = float64(f32)
		} else {
			v.float = f
		}
	case "text":
		var s string
		if err := msgpack.Unmarshal(raw.Value, &s); err != nil {
			return fmt.Errorf("hrana: text value: %w", err)
		}
		v.text = s
	case "blob":
		b, err := base64.StdEncoding.DecodeString(raw.Base64)
		if err != nil {
			return fmt.Errorf("hrana: blob value: %w", err)
		}
		v.blob = b
	default:
		return fmt.Errorf("hrana: unknown value type %q", raw.Type)
	}
	return nil
}

// ─── Statement ───────────────────────────────────────────────────────────────

type namedArg struct {
	Name  string `json:"name" msgpack:"name"`
	Value Value  `json:"value" msgpack:"value"`
}

type stmt struct {
	SQL       string     `json:"sql" msgpack:"sql"`
	Args      []Value    `json:"args,omitempty" msgpack:"args,omitempty"`
	NamedArgs []namedArg `json:"named_args,omitempty" msgpack:"named_args,omitempty"`
	WantRows  bool       `json:"want_rows" msgpack:"want_rows"`
}

// ─── Column ──────────────────────────────────────────────────────────────────

type col struct {
	Name     *string `json:"name" msgpack:"name"`
	Decltype *string `json:"decltype,omitempty" msgpack:"decltype,omitempty"`
}

// ─── Statement result ────────────────────────────────────────────────────────

type stmtResult struct {
	Cols             []col     `json:"cols" msgpack:"cols"`
	Rows             [][]Value `json:"rows" msgpack:"rows"`
	AffectedRowCount uint64    `json:"affected_row_count" msgpack:"affected_row_count"`
	LastInsertRowid  *string   `json:"last_insert_rowid" msgpack:"last_insert_rowid"`
}

// ─── Pipeline wire types ─────────────────────────────────────────────────────

type pipelineReq struct {
	Baton    *string         `json:"baton,omitempty"`
	Requests []streamRequest `json:"requests"`
}

type streamRequest struct {
	Type string `json:"type"`
	Stmt *stmt  `json:"stmt,omitempty"`
}

type pipelineResp struct {
	Baton   *string        `json:"baton"`
	BaseURL *string        `json:"base_url"`
	Results []streamResult `json:"results"`
}

type streamResult struct {
	Type     string          `json:"type"` // "ok" or "error"
	Response *streamResponse `json:"response,omitempty"`
	Error    *hranaError     `json:"error,omitempty"`
}

type streamResponse struct {
	Type   string      `json:"type"` // "execute", "close"
	Result *stmtResult `json:"result,omitempty"`
}

type hranaError struct {
	Message string  `json:"message" msgpack:"message"`
	Code    *string `json:"code,omitempty" msgpack:"code,omitempty"`
}
