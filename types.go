package hrana

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/vmihailenco/msgpack/v5"
)

// ─── Wire types ──────────────────────────────────────────────────────────────

// Error is the Hrana protocol error type.
type Error struct {
	Message string  `json:"message" msgpack:"message"`
	Code    *string `json:"code,omitempty" msgpack:"code,omitempty"`
}

func (e *Error) Error() string { return e.Message }

func newError(msg string) *Error { return &Error{Message: msg} }

// Value represents a single SQLite value in the Hrana protocol.
// Custom marshal/unmarshal handles the string-encoded int64 and base64 blob.
type Value struct {
	// Exactly one of the following native fields is set after unmarshalling.
	IsNull  bool
	Integer int64
	Float   float64
	Text    string
	Blob    []byte

	// set when this value carries a float or integer (used internally)
	typ string
}

func NullValue() Value           { return Value{IsNull: true, typ: "null"} }
func IntValue(i int64) Value     { return Value{Integer: i, typ: "integer"} }
func FloatValue(f float64) Value { return Value{Float: f, typ: "float"} }
func TextValue(s string) Value   { return Value{Text: s, typ: "text"} }
func BlobValue(b []byte) Value   { return Value{Blob: b, typ: "blob"} }

func (v Value) MarshalJSON() ([]byte, error) {
	switch v.typ {
	case "null", "":
		return []byte(`{"type":"null"}`), nil
	case "integer":
		s := strconv.FormatInt(v.Integer, 10)
		return json.Marshal(map[string]string{"type": "integer", "value": s})
	case "float":
		return json.Marshal(map[string]any{"type": "float", "value": v.Float})
	case "text":
		return json.Marshal(map[string]string{"type": "text", "value": v.Text})
	case "blob":
		return json.Marshal(map[string]string{"type": "blob", "base64": base64.StdEncoding.EncodeToString(v.Blob)})
	default:
		return nil, fmt.Errorf("unknown value type: %s", v.typ)
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
		v.IsNull = true
	case "integer":
		var s string
		if err := json.Unmarshal(raw.Value, &s); err != nil {
			return fmt.Errorf("hrana: integer value must be a string: %w", err)
		}
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("hrana: invalid integer value %q: %w", s, err)
		}
		v.Integer = i
	case "float":
		var f float64
		if err := json.Unmarshal(raw.Value, &f); err != nil {
			return fmt.Errorf("hrana: float value must be a number: %w", err)
		}
		v.Float = f
	case "text":
		var s string
		if err := json.Unmarshal(raw.Value, &s); err != nil {
			return fmt.Errorf("hrana: text value must be a string: %w", err)
		}
		v.Text = s
	case "blob":
		b, err := base64.StdEncoding.DecodeString(raw.Base64)
		if err != nil {
			return fmt.Errorf("hrana: invalid base64 blob: %w", err)
		}
		v.Blob = b
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
		s := strconv.FormatInt(v.Integer, 10)
		return msgpack.Marshal(map[string]string{"type": "integer", "value": s})
	case "float":
		return msgpack.Marshal(map[string]any{"type": "float", "value": v.Float})
	case "text":
		return msgpack.Marshal(map[string]string{"type": "text", "value": v.Text})
	case "blob":
		return msgpack.Marshal(map[string]string{"type": "blob", "base64": base64.StdEncoding.EncodeToString(v.Blob)})
	default:
		return nil, fmt.Errorf("unknown value type: %s", v.typ)
	}
}

func (v *Value) UnmarshalMsgpack(data []byte) error {
	var raw struct {
		Type   string `msgpack:"type"`
		Value  any    `msgpack:"value"`
		Base64 string `msgpack:"base64"`
	}
	if err := msgpack.Unmarshal(data, &raw); err != nil {
		return err
	}
	v.typ = raw.Type
	switch raw.Type {
	case "null":
		v.IsNull = true
	case "integer":
		s, ok := raw.Value.(string)
		if !ok {
			return fmt.Errorf("hrana: integer value must be a string, got %T", raw.Value)
		}
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("hrana: invalid integer value %q: %w", s, err)
		}
		v.Integer = i
	case "float":
		switch f := raw.Value.(type) {
		case float64:
			v.Float = f
		case float32:
			v.Float = float64(f)
		default:
			return fmt.Errorf("hrana: float value must be a number, got %T", raw.Value)
		}
	case "text":
		s, ok := raw.Value.(string)
		if !ok {
			return fmt.Errorf("hrana: text value must be a string, got %T", raw.Value)
		}
		v.Text = s
	case "blob":
		b, err := base64.StdEncoding.DecodeString(raw.Base64)
		if err != nil {
			return fmt.Errorf("hrana: invalid base64 blob: %w", err)
		}
		v.Blob = b
	default:
		return fmt.Errorf("hrana: unknown value type %q", raw.Type)
	}
	return nil
}

// ─── Statement types ─────────────────────────────────────────────────────────

type NamedArg struct {
	Name  string `json:"name" msgpack:"name"`
	Value Value  `json:"value" msgpack:"value"`
}

type Stmt struct {
	SQL       *string    `json:"sql,omitempty" msgpack:"sql,omitempty"`
	SQLId     *int32     `json:"sql_id,omitempty" msgpack:"sql_id,omitempty"`
	Args      []Value    `json:"args,omitempty" msgpack:"args,omitempty"`
	NamedArgs []NamedArg `json:"named_args,omitempty" msgpack:"named_args,omitempty"`
	WantRows  *bool      `json:"want_rows,omitempty" msgpack:"want_rows,omitempty"`
}

func (s *Stmt) wantRows() bool {
	if s.WantRows == nil {
		return true
	}
	return *s.WantRows
}

type Col struct {
	Name     *string `json:"name" msgpack:"name"`
	Decltype *string `json:"decltype,omitempty" msgpack:"decltype,omitempty"`
}

type StmtResult struct {
	Cols             []Col     `json:"cols" msgpack:"cols"`
	Rows             [][]Value `json:"rows" msgpack:"rows"`
	AffectedRowCount uint64    `json:"affected_row_count" msgpack:"affected_row_count"`
	LastInsertRowid  *string   `json:"last_insert_rowid" msgpack:"last_insert_rowid"`
	RowsRead         uint64    `json:"rows_read" msgpack:"rows_read"`
	RowsWritten      uint64    `json:"rows_written" msgpack:"rows_written"`
	QueryDurationMs  float64   `json:"query_duration_ms" msgpack:"query_duration_ms"`
}

// ─── Batch types ─────────────────────────────────────────────────────────────

type Batch struct {
	Steps []BatchStep `json:"steps" msgpack:"steps"`
}

type BatchStep struct {
	Condition *BatchCond `json:"condition,omitempty" msgpack:"condition,omitempty"`
	Stmt      Stmt       `json:"stmt" msgpack:"stmt"`
}

type BatchResult struct {
	StepResults []*StmtResult `json:"step_results" msgpack:"step_results"`
	StepErrors  []*Error      `json:"step_errors" msgpack:"step_errors"`
}

// BatchCond is a recursive condition expression.
type BatchCond struct {
	Type  string      `json:"type" msgpack:"type"`
	Step  *uint32     `json:"step,omitempty" msgpack:"step,omitempty"`
	Cond  *BatchCond  `json:"cond,omitempty" msgpack:"cond,omitempty"`
	Conds []BatchCond `json:"conds,omitempty" msgpack:"conds,omitempty"`
}

// ─── Describe types ──────────────────────────────────────────────────────────

type DescribeResult struct {
	Params     []DescribeParam `json:"params" msgpack:"params"`
	Cols       []DescribeCol   `json:"cols" msgpack:"cols"`
	IsExplain  bool            `json:"is_explain" msgpack:"is_explain"`
	IsReadonly bool            `json:"is_readonly" msgpack:"is_readonly"`
}

type DescribeParam struct {
	Name *string `json:"name" msgpack:"name"`
}

type DescribeCol struct {
	Name     string  `json:"name" msgpack:"name"`
	Decltype *string `json:"decltype" msgpack:"decltype"`
}

// ─── Cursor entry types ──────────────────────────────────────────────────────

// CursorEntry is encoded as newline-delimited JSON; the type discriminator
// is in the "type" field.
type CursorEntry struct {
	Type string `json:"type" msgpack:"type"`

	// step_begin
	Step *uint32 `json:"step,omitempty" msgpack:"step,omitempty"`
	Cols []Col   `json:"cols,omitempty" msgpack:"cols,omitempty"`

	// step_end
	AffectedRowCount *uint32 `json:"affected_row_count,omitempty" msgpack:"affected_row_count,omitempty"`
	LastInsertRowid  *string `json:"last_insert_rowid,omitempty" msgpack:"last_insert_rowid,omitempty"`

	// row
	Row []Value `json:"row,omitempty" msgpack:"row,omitempty"`

	// step_error / error
	Error *Error `json:"error,omitempty" msgpack:"error,omitempty"`
}

// ─── WebSocket message types ─────────────────────────────────────────────────

type HelloMsg struct {
	Type string  `json:"type" msgpack:"type"` // "hello"
	JWT  *string `json:"jwt" msgpack:"jwt"`
}

type HelloOkMsg struct {
	Type string `json:"type" msgpack:"type"` // "hello_ok"
}

type HelloErrorMsg struct {
	Type  string `json:"type" msgpack:"type"` // "hello_error"
	Error Error  `json:"error" msgpack:"error"`
}

// RequestMsg wraps any Request payload.
type RequestMsg struct {
	Type      string          `json:"type"` // "request"
	RequestID int32           `json:"request_id"`
	Request   json.RawMessage `json:"request"`
}

type ResponseOkMsg struct {
	Type      string `json:"type" msgpack:"type"` // "response_ok"
	RequestID int32  `json:"request_id" msgpack:"request_id"`
	Response  any    `json:"response" msgpack:"response"`
}

type ResponseErrorMsg struct {
	Type      string `json:"type" msgpack:"type"` // "response_error"
	RequestID int32  `json:"request_id" msgpack:"request_id"`
	Error     Error  `json:"error" msgpack:"error"`
}

// ─── WS request/response payloads ───────────────────────────────────────────

type OpenStreamReq struct {
	Type     string `json:"type" msgpack:"type"`
	StreamID int32  `json:"stream_id" msgpack:"stream_id"`
}
type OpenStreamResp struct {
	Type string `json:"type" msgpack:"type"`
}

type CloseStreamReq struct {
	Type     string `json:"type" msgpack:"type"`
	StreamID int32  `json:"stream_id" msgpack:"stream_id"`
}
type CloseStreamResp struct {
	Type string `json:"type" msgpack:"type"`
}

type ExecuteReq struct {
	Type     string `json:"type" msgpack:"type"`
	StreamID int32  `json:"stream_id" msgpack:"stream_id"`
	Stmt     Stmt   `json:"stmt" msgpack:"stmt"`
}
type ExecuteResp struct {
	Type   string     `json:"type" msgpack:"type"`
	Result StmtResult `json:"result" msgpack:"result"`
}

type BatchReq struct {
	Type     string `json:"type" msgpack:"type"`
	StreamID int32  `json:"stream_id" msgpack:"stream_id"`
	Batch    Batch  `json:"batch" msgpack:"batch"`
}
type BatchResp struct {
	Type   string      `json:"type" msgpack:"type"`
	Result BatchResult `json:"result" msgpack:"result"`
}

type StoreSqlReq struct {
	Type  string `json:"type" msgpack:"type"`
	SQLID int32  `json:"sql_id" msgpack:"sql_id"`
	SQL   string `json:"sql" msgpack:"sql"`
}
type StoreSqlResp struct {
	Type string `json:"type" msgpack:"type"`
}

type CloseSqlReq struct {
	Type  string `json:"type" msgpack:"type"`
	SQLID int32  `json:"sql_id" msgpack:"sql_id"`
}
type CloseSqlResp struct {
	Type string `json:"type" msgpack:"type"`
}

type SequenceReq struct {
	Type     string  `json:"type" msgpack:"type"`
	StreamID int32   `json:"stream_id" msgpack:"stream_id"`
	SQL      *string `json:"sql,omitempty" msgpack:"sql,omitempty"`
	SQLId    *int32  `json:"sql_id,omitempty" msgpack:"sql_id,omitempty"`
}
type SequenceResp struct {
	Type string `json:"type" msgpack:"type"`
}

type DescribeReq struct {
	Type     string  `json:"type" msgpack:"type"`
	StreamID int32   `json:"stream_id" msgpack:"stream_id"`
	SQL      *string `json:"sql,omitempty" msgpack:"sql,omitempty"`
	SQLId    *int32  `json:"sql_id,omitempty" msgpack:"sql_id,omitempty"`
}
type DescribeResp struct {
	Type   string         `json:"type" msgpack:"type"`
	Result DescribeResult `json:"result" msgpack:"result"`
}

type GetAutocommitReq struct {
	Type     string `json:"type" msgpack:"type"`
	StreamID int32  `json:"stream_id" msgpack:"stream_id"`
}
type GetAutocommitResp struct {
	Type         string `json:"type" msgpack:"type"`
	IsAutocommit bool   `json:"is_autocommit" msgpack:"is_autocommit"`
}

type OpenCursorReq struct {
	Type     string `json:"type" msgpack:"type"`
	StreamID int32  `json:"stream_id" msgpack:"stream_id"`
	CursorID int32  `json:"cursor_id" msgpack:"cursor_id"`
	Batch    Batch  `json:"batch" msgpack:"batch"`
}
type OpenCursorResp struct {
	Type string `json:"type" msgpack:"type"`
}

type CloseCursorReq struct {
	Type     string `json:"type" msgpack:"type"`
	CursorID int32  `json:"cursor_id" msgpack:"cursor_id"`
}
type CloseCursorResp struct {
	Type string `json:"type" msgpack:"type"`
}

type FetchCursorReq struct {
	Type     string `json:"type" msgpack:"type"`
	CursorID int32  `json:"cursor_id" msgpack:"cursor_id"`
	MaxCount uint32 `json:"max_count" msgpack:"max_count"`
}
type FetchCursorResp struct {
	Type    string        `json:"type" msgpack:"type"`
	Entries []CursorEntry `json:"entries" msgpack:"entries"`
	Done    bool          `json:"done" msgpack:"done"`
}

// ─── HTTP pipeline types ─────────────────────────────────────────────────────

type PipelineReqBody struct {
	Baton    *string         `json:"baton"`
	Requests []StreamRequest `json:"requests"`
}

type PipelineRespBody struct {
	Baton   *string        `json:"baton"`
	BaseURL *string        `json:"base_url"`
	Results []StreamResult `json:"results"`
}

type StreamResult struct {
	Type     string          `json:"type"` // "ok" or "error"
	Response *StreamResponse `json:"response,omitempty"`
	Error    *Error          `json:"error,omitempty"`
}

// StreamRequest is a discriminated union decoded by its "type" field.
type StreamRequest struct {
	Type  string          `json:"type"`
	Inner json.RawMessage `json:"-"`

	// Parsed inner depending on Type:
	Execute       *ExecuteStreamReq
	Batch         *BatchStreamReq
	Sequence      *SequenceStreamReq
	Describe      *DescribeStreamReq
	StoreSql      *StoreSqlStreamReq
	CloseSql      *CloseSqlStreamReq
	GetAutocommit *GetAutocommitStreamReq
	// "close" has no extra fields
}

func (r *StreamRequest) UnmarshalJSON(data []byte) error {
	var base struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &base); err != nil {
		return err
	}
	r.Type = base.Type
	switch base.Type {
	case "close":
		// no extra fields
	case "execute":
		r.Execute = new(ExecuteStreamReq)
		return json.Unmarshal(data, r.Execute)
	case "batch":
		r.Batch = new(BatchStreamReq)
		return json.Unmarshal(data, r.Batch)
	case "sequence":
		r.Sequence = new(SequenceStreamReq)
		return json.Unmarshal(data, r.Sequence)
	case "describe":
		r.Describe = new(DescribeStreamReq)
		return json.Unmarshal(data, r.Describe)
	case "store_sql":
		r.StoreSql = new(StoreSqlStreamReq)
		return json.Unmarshal(data, r.StoreSql)
	case "close_sql":
		r.CloseSql = new(CloseSqlStreamReq)
		return json.Unmarshal(data, r.CloseSql)
	case "get_autocommit":
		r.GetAutocommit = new(GetAutocommitStreamReq)
	default:
		return fmt.Errorf("hrana: unknown stream request type %q", base.Type)
	}
	return nil
}

// StreamResponse wraps a response payload; the Type field matches the request.
type StreamResponse struct {
	Type string `json:"type"`
	// Exactly one of the following is non-nil:
	ExecuteResult  *StmtResult     `json:"result,omitempty"`
	BatchResult    *BatchResult    `json:"result2,omitempty"` // marshalled as "result"
	DescribeResult *DescribeResult `json:"result3,omitempty"` // marshalled as "result"
	IsAutocommit   *bool           `json:"is_autocommit,omitempty"`
}

func (sr StreamResponse) MarshalJSON() ([]byte, error) {
	switch sr.Type {
	case "close", "sequence", "store_sql", "close_sql":
		return json.Marshal(map[string]string{"type": sr.Type})
	case "execute":
		return json.Marshal(map[string]any{"type": sr.Type, "result": sr.ExecuteResult})
	case "batch":
		return json.Marshal(map[string]any{"type": sr.Type, "result": sr.BatchResult})
	case "describe":
		return json.Marshal(map[string]any{"type": sr.Type, "result": sr.DescribeResult})
	case "get_autocommit":
		return json.Marshal(map[string]any{"type": sr.Type, "is_autocommit": sr.IsAutocommit})
	default:
		return nil, fmt.Errorf("hrana: unknown stream response type %q", sr.Type)
	}
}

// HTTP stream request sub-types
type ExecuteStreamReq struct {
	Type string `json:"type"`
	Stmt Stmt   `json:"stmt"`
}
type BatchStreamReq struct {
	Type  string `json:"type"`
	Batch Batch  `json:"batch"`
}
type SequenceStreamReq struct {
	Type  string  `json:"type"`
	SQL   *string `json:"sql,omitempty"`
	SQLId *int32  `json:"sql_id,omitempty"`
}
type DescribeStreamReq struct {
	Type  string  `json:"type"`
	SQL   *string `json:"sql,omitempty"`
	SQLId *int32  `json:"sql_id,omitempty"`
}
type StoreSqlStreamReq struct {
	Type  string `json:"type"`
	SQLID int32  `json:"sql_id"`
	SQL   string `json:"sql"`
}
type CloseSqlStreamReq struct {
	Type  string `json:"type"`
	SQLID int32  `json:"sql_id"`
}
type GetAutocommitStreamReq struct {
	Type string `json:"type"`
}

// HTTP v3 cursor types
type CursorReqBody struct {
	Baton *string `json:"baton"`
	Batch Batch   `json:"batch"`
}

type CursorRespBody struct {
	Baton   *string `json:"baton"`
	BaseURL *string `json:"base_url"`
}

// ─── HTTP v1 request/response bodies ────────────────────────────────────────

type V1ExecuteReqBody struct {
	Stmt Stmt `json:"stmt"`
}

type V1ExecuteRespBody struct {
	Result StmtResult `json:"result"`
}

type V1BatchReqBody struct {
	Batch Batch `json:"batch"`
}

type V1BatchRespBody struct {
	Result BatchResult `json:"result"`
}
