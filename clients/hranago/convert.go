package hranago

import (
	"database/sql/driver"
	"fmt"
	"time"
)

// buildStmt converts a SQL string and driver.NamedValue args into a Hrana stmt.
func buildStmt(sql string, args []driver.NamedValue, wantRows bool) (*stmt, error) {
	s := &stmt{SQL: sql, WantRows: wantRows}

	if len(args) == 0 {
		return s, nil
	}

	// If any arg is named, use named_args; otherwise use positional args.
	hasNamed := false
	for _, a := range args {
		if a.Name != "" {
			hasNamed = true
			break
		}
	}

	if hasNamed {
		s.NamedArgs = make([]namedArg, len(args))
		for i, a := range args {
			v, err := driverValueToHrana(a.Value)
			if err != nil {
				return nil, fmt.Errorf("hrana: arg %d: %w", i, err)
			}
			s.NamedArgs[i] = namedArg{Name: a.Name, Value: v}
		}
	} else {
		s.Args = make([]Value, len(args))
		for i, a := range args {
			v, err := driverValueToHrana(a.Value)
			if err != nil {
				return nil, fmt.Errorf("hrana: arg %d: %w", i, err)
			}
			s.Args[i] = v
		}
	}

	return s, nil
}

// driverValueToHrana converts a driver.Value to a Hrana Value.
// Supported types: nil, int64, float64, bool, string, []byte, time.Time.
func driverValueToHrana(v driver.Value) (Value, error) {
	if v == nil {
		return nullValue(), nil
	}
	switch t := v.(type) {
	case int64:
		return integerValue(t), nil
	case float64:
		return floatValue(t), nil
	case bool:
		if t {
			return integerValue(1), nil
		}
		return integerValue(0), nil
	case string:
		return textValue(t), nil
	case []byte:
		return blobValue(t), nil
	case time.Time:
		return textValue(t.UTC().Format(time.RFC3339Nano)), nil
	default:
		return Value{}, fmt.Errorf("hrana: unsupported driver.Value type %T", v)
	}
}

// hranaValueToDriver converts a Hrana Value to a driver.Value.
func hranaValueToDriver(v Value) driver.Value {
	switch v.typ {
	case "null", "":
		return nil
	case "integer":
		return v.integer
	case "float":
		return v.float
	case "text":
		return v.text
	case "blob":
		return v.blob
	default:
		return nil
	}
}

// valuesToNamedValues wraps a []driver.Value slice into []driver.NamedValue
// for compatibility with the context-aware exec/query methods.
func valuesToNamedValues(vals []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(vals))
	for i, v := range vals {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out
}

// execResult implements driver.Result.
type execResult struct {
	result *stmtResult
}

func (r *execResult) LastInsertId() (int64, error) {
	if r.result.LastInsertRowid == nil {
		return 0, nil
	}
	var id int64
	_, err := fmt.Sscanf(*r.result.LastInsertRowid, "%d", &id)
	return id, err
}

func (r *execResult) RowsAffected() (int64, error) {
	return int64(r.result.AffectedRowCount), nil
}
