package hrana

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// executeStmt translates a Hrana Stmt into a database/sql call and returns the
// StmtResult. storedSQL is the per-stream (HTTP) or per-session (WS) SQL map.
func executeStmt(ctx context.Context, stream *Stream, stmt *Stmt, storedSQL map[int32]string) (*StmtResult, error) {
	sqlText, err := resolveStmtSQL(stmt, storedSQL)
	if err != nil {
		return nil, err
	}

	if stream.Mode == ModeReadOnly && isWriteStatement(sqlText) {
		return nil, fmt.Errorf("hrana: write operations are not permitted in readonly mode")
	}

	args, err := buildArgs(stmt)
	if err != nil {
		return nil, err
	}

	start := time.Now()

	if !stmt.wantRows() {
		res, execErr := stream.Conn.ExecContext(ctx, sqlText, args...)
		if execErr != nil {
			return nil, execErr
		}
		return execResultToStmtResult(res, time.Since(start))
	}

	rows, err := stream.Conn.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rowsToStmtResult(rows, time.Since(start))
}

func resolveStmtSQL(stmt *Stmt, storedSQL map[int32]string) (string, error) {
	if stmt.SQL != nil && stmt.SQLId != nil {
		return "", fmt.Errorf("hrana: exactly one of sql and sql_id must be set in stmt")
	}
	if stmt.SQL != nil {
		return *stmt.SQL, nil
	}
	if stmt.SQLId != nil {
		s, ok := storedSQL[*stmt.SQLId]
		if !ok {
			return "", fmt.Errorf("hrana: sql_id %d not found", *stmt.SQLId)
		}
		return s, nil
	}
	return "", fmt.Errorf("hrana: one of sql or sql_id must be set in stmt")
}

func buildArgs(stmt *Stmt) ([]any, error) {
	if len(stmt.NamedArgs) > 0 {
		args := make([]any, len(stmt.NamedArgs))
		for i, na := range stmt.NamedArgs {
			name := na.Name
			if !strings.HasPrefix(name, ":") && !strings.HasPrefix(name, "@") && !strings.HasPrefix(name, "$") {
				name = ":" + name
			}
			v, err := valueToAny(na.Value)
			if err != nil {
				return nil, err
			}
			// sql.Named uses the bare name (without prefix)
			args[i] = sql.Named(name[1:], v)
		}
		return args, nil
	}

	args := make([]any, len(stmt.Args))
	for i, v := range stmt.Args {
		a, err := valueToAny(v)
		if err != nil {
			return nil, err
		}
		args[i] = a
	}
	return args, nil
}

func valueToAny(v Value) (any, error) {
	switch v.typ {
	case "null", "":
		return nil, nil
	case "integer":
		return v.Integer, nil
	case "float":
		return v.Float, nil
	case "text":
		return v.Text, nil
	case "blob":
		return v.Blob, nil
	default:
		return nil, fmt.Errorf("hrana: unknown value type %q", v.typ)
	}
}

func execResultToStmtResult(res sql.Result, dur time.Duration) (*StmtResult, error) {
	affected, _ := res.RowsAffected()
	lastID, _ := res.LastInsertId()

	var lastRowid *string
	if lastID != 0 {
		s := fmt.Sprintf("%d", lastID)
		lastRowid = &s
	}

	return &StmtResult{
		Cols:             []Col{},
		Rows:             [][]Value{},
		AffectedRowCount: uint64(affected),
		LastInsertRowid:  lastRowid,
		RowsWritten:      uint64(affected),
		QueryDurationMs:  float64(dur.Microseconds()) / 1000.0,
	}, nil
}

func rowsToStmtResult(rows *sql.Rows, dur time.Duration) (*StmtResult, error) {
	colNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("hrana: reading column names: %w", err)
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("hrana: reading column types: %w", err)
	}

	cols := make([]Col, len(colNames))
	for i, name := range colNames {
		n := name
		col := Col{Name: &n}
		if i < len(colTypes) {
			dt := colTypes[i].DatabaseTypeName()
			if dt != "" {
				col.Decltype = &dt
			}
		}
		cols[i] = col
	}

	scanDest := make([]any, len(colNames))
	scanPtrs := make([]any, len(colNames))
	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	var resultRows [][]Value
	var rowsRead uint64
	for rows.Next() {
		rowsRead++
		if err := rows.Scan(scanPtrs...); err != nil {
			return nil, fmt.Errorf("hrana: scanning row: %w", err)
		}
		row := make([]Value, len(colNames))
		for i, raw := range scanDest {
			row[i] = anyToValue(raw)
		}
		resultRows = append(resultRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("hrana: iterating rows: %w", err)
	}

	if resultRows == nil {
		resultRows = [][]Value{}
	}

	return &StmtResult{
		Cols:            cols,
		Rows:            resultRows,
		RowsRead:        rowsRead,
		QueryDurationMs: float64(dur.Microseconds()) / 1000.0,
	}, nil
}

func anyToValue(v any) Value {
	if v == nil {
		return NullValue()
	}
	switch t := v.(type) {
	case int64:
		return IntValue(t)
	case float64:
		return FloatValue(t)
	case string:
		return TextValue(t)
	case []byte:
		return BlobValue(t)
	case bool:
		if t {
			return IntValue(1)
		}
		return IntValue(0)
	default:
		return TextValue(fmt.Sprintf("%v", v))
	}
}

// executeBatch runs all steps in batch order, evaluating conditions, and
// returns a BatchResult. storedSQL is a per-scope SQL map.
func executeBatch(ctx context.Context, stream *Stream, batch *Batch, storedSQL map[int32]string) (*BatchResult, error) {
	stepResults := make([]*StmtResult, len(batch.Steps))
	stepErrors := make([]*Error, len(batch.Steps))

	for i, step := range batch.Steps {
		if step.Condition != nil {
			ok, err := evalBatchCond(step.Condition, stepResults, stepErrors)
			if err != nil {
				return nil, fmt.Errorf("hrana: batch cond at step %d: %w", i, err)
			}
			if !ok {
				continue
			}
		}

		result, err := executeStmt(ctx, stream, &step.Stmt, storedSQL)
		if err != nil {
			stepErrors[i] = &Error{Message: err.Error()}
		} else {
			stepResults[i] = result
		}
	}

	return &BatchResult{StepResults: stepResults, StepErrors: stepErrors}, nil
}

func evalBatchCond(cond *BatchCond, results []*StmtResult, errors []*Error) (bool, error) {
	switch cond.Type {
	case "ok":
		if cond.Step == nil {
			return false, fmt.Errorf("hrana: batch cond 'ok' missing step")
		}
		idx := int(*cond.Step)
		if idx >= len(results) {
			return false, nil
		}
		return results[idx] != nil, nil

	case "error":
		if cond.Step == nil {
			return false, fmt.Errorf("hrana: batch cond 'error' missing step")
		}
		idx := int(*cond.Step)
		if idx >= len(errors) {
			return false, nil
		}
		return errors[idx] != nil, nil

	case "not":
		if cond.Cond == nil {
			return false, fmt.Errorf("hrana: batch cond 'not' missing inner cond")
		}
		v, err := evalBatchCond(cond.Cond, results, errors)
		return !v, err

	case "and":
		for i := range cond.Conds {
			v, err := evalBatchCond(&cond.Conds[i], results, errors)
			if err != nil || !v {
				return false, err
			}
		}
		return true, nil

	case "or":
		for i := range cond.Conds {
			v, err := evalBatchCond(&cond.Conds[i], results, errors)
			if err != nil {
				return false, err
			}
			if v {
				return true, nil
			}
		}
		return false, nil

	case "is_autocommit":
		// Without driver introspection we default to true (most common case).
		// A production implementation would check sqlite3_get_autocommit via CGo.
		return true, nil

	default:
		return false, fmt.Errorf("hrana: unknown batch condition type %q", cond.Type)
	}
}

// executeSequence runs a semicolon-separated SQL script on the stream,
// ignoring any returned rows.
func executeSequence(ctx context.Context, stream *Stream, sqlText string) error {
	for _, stmt := range strings.Split(sqlText, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if stream.Mode == ModeReadOnly && isWriteStatement(stmt) {
			return fmt.Errorf("hrana: write operations are not permitted in readonly mode")
		}
		if _, err := stream.Conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("hrana: sequence error: %w", err)
		}
	}
	return nil
}

// describeStmt parses a statement and returns metadata about its parameters and
// result columns using EXPLAIN QUERY PLAN (SQLite only).
func describeStmt(ctx context.Context, stream *Stream, sqlText string) (*DescribeResult, error) {
	rows, err := stream.Conn.QueryContext(ctx, sqlText)
	if err != nil {
		return &DescribeResult{Params: []DescribeParam{}, Cols: []DescribeCol{}}, nil
	}
	defer rows.Close()

	colNames, _ := rows.Columns()
	colTypes, _ := rows.ColumnTypes()

	cols := make([]DescribeCol, len(colNames))
	for i, n := range colNames {
		col := DescribeCol{Name: n}
		if i < len(colTypes) {
			dt := colTypes[i].DatabaseTypeName()
			if dt != "" {
				col.Decltype = &dt
			}
		}
		cols[i] = col
	}

	upper := strings.ToUpper(strings.TrimSpace(sqlText))
	return &DescribeResult{
		Params:     []DescribeParam{},
		Cols:       cols,
		IsExplain:  strings.HasPrefix(upper, "EXPLAIN"),
		IsReadonly: isReadonlyStatement(upper),
	}, nil
}

func isReadonlyStatement(upper string) bool {
	for _, prefix := range []string{"SELECT", "EXPLAIN", "PRAGMA", "WITH"} {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	return false
}

// executeBatchCursor runs batch steps and emits CursorEntry values for
// incremental (streaming) delivery.
func executeBatchCursor(ctx context.Context, stream *Stream, batch *Batch, storedSQL map[int32]string) ([]CursorEntry, error) {
	var entries []CursorEntry

	stepResults := make([]*StmtResult, len(batch.Steps))
	stepErrors := make([]*Error, len(batch.Steps))

	for i, step := range batch.Steps {
		stepIdx := uint32(i)

		if step.Condition != nil {
			ok, err := evalBatchCond(step.Condition, stepResults, stepErrors)
			if err != nil {
				e := &Error{Message: err.Error()}
				entries = append(entries, CursorEntry{Type: "error", Error: e})
				return entries, nil
			}
			if !ok {
				continue
			}
		}

		sqlText, err := resolveStmtSQL(&step.Stmt, storedSQL)
		if err != nil {
			e := &Error{Message: err.Error()}
			entries = append(entries, CursorEntry{Type: "step_error", Step: &stepIdx, Error: e})
			stepErrors[i] = e
			continue
		}

		args, err := buildArgs(&step.Stmt)
		if err != nil {
			e := &Error{Message: err.Error()}
			entries = append(entries, CursorEntry{Type: "step_error", Step: &stepIdx, Error: e})
			stepErrors[i] = e
			continue
		}

		if !step.Stmt.wantRows() {
			res, execErr := stream.Conn.ExecContext(ctx, sqlText, args...)
			if execErr != nil {
				e := &Error{Message: execErr.Error()}
				entries = append(entries, CursorEntry{Type: "step_error", Step: &stepIdx, Error: e})
				stepErrors[i] = e
				continue
			}
			affected, _ := res.RowsAffected()
			lastID, _ := res.LastInsertId()
			affU := uint32(affected)
			var lastRowid *string
			if lastID != 0 {
				s := fmt.Sprintf("%d", lastID)
				lastRowid = &s
			}
			entries = append(entries, CursorEntry{Type: "step_begin", Step: &stepIdx, Cols: []Col{}})
			entries = append(entries, CursorEntry{Type: "step_end", AffectedRowCount: &affU, LastInsertRowid: lastRowid})
			stepResults[i] = &StmtResult{AffectedRowCount: uint64(affected)}
			continue
		}

		rows, qErr := stream.Conn.QueryContext(ctx, sqlText, args...)
		if qErr != nil {
			e := &Error{Message: qErr.Error()}
			entries = append(entries, CursorEntry{Type: "step_error", Step: &stepIdx, Error: e})
			stepErrors[i] = e
			continue
		}

		colNames, _ := rows.Columns()
		colTypes, _ := rows.ColumnTypes()
		cols := make([]Col, len(colNames))
		for j, n := range colNames {
			name := n
			col := Col{Name: &name}
			if j < len(colTypes) {
				dt := colTypes[j].DatabaseTypeName()
				if dt != "" {
					col.Decltype = &dt
				}
			}
			cols[j] = col
		}

		entries = append(entries, CursorEntry{Type: "step_begin", Step: &stepIdx, Cols: cols})

		scanDest := make([]any, len(colNames))
		scanPtrs := make([]any, len(colNames))
		for j := range scanDest {
			scanPtrs[j] = &scanDest[j]
		}

		var rowsRead uint64
		var scanErr error
		for rows.Next() {
			rowsRead++
			if scanErr = rows.Scan(scanPtrs...); scanErr != nil {
				break
			}
			row := make([]Value, len(colNames))
			for j, raw := range scanDest {
				row[j] = anyToValue(raw)
			}
			entries = append(entries, CursorEntry{Type: "row", Row: row})
		}
		iterErr := rows.Err()
		rows.Close()

		if scanErr != nil || iterErr != nil {
			msg := ""
			if scanErr != nil {
				msg = scanErr.Error()
			} else {
				msg = iterErr.Error()
			}
			e := &Error{Message: msg}
			entries = append(entries, CursorEntry{Type: "step_error", Step: &stepIdx, Error: e})
			stepErrors[i] = e
			continue
		}

		var affZero uint32
		entries = append(entries, CursorEntry{Type: "step_end", AffectedRowCount: &affZero})
		stepResults[i] = &StmtResult{Rows: [][]Value{}, RowsRead: rowsRead}
	}

	return entries, nil
}
