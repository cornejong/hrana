package hranago

import (
	"context"
	"database/sql/driver"
)

// preparedStmt implements driver.Stmt, driver.StmtExecContext, and
// driver.StmtQueryContext. The SQL is stored client-side; no server-side
// "store_sql" request is made (keeps the implementation simple and stateless).
type preparedStmt struct {
	conn stmtExecutor
	sql  string
}

// Close is a no-op because we do not allocate any server-side resources.
func (s *preparedStmt) Close() error { return nil }

// NumInput returns -1 to indicate a variable number of parameters.
func (s *preparedStmt) NumInput() int { return -1 }

// Exec implements driver.Stmt (legacy, no context).
func (s *preparedStmt) Exec(args []driver.Value) (driver.Result, error) {
	nvArgs := valuesToNamedValues(args)
	return s.ExecContext(context.Background(), nvArgs)
}

// Query implements driver.Stmt (legacy, no context).
func (s *preparedStmt) Query(args []driver.Value) (driver.Rows, error) {
	nvArgs := valuesToNamedValues(args)
	return s.QueryContext(context.Background(), nvArgs)
}

// ExecContext implements driver.StmtExecContext.
func (s *preparedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	hstmt, err := buildStmt(s.sql, args, false)
	if err != nil {
		return nil, err
	}
	result, err := s.conn.execStatement(ctx, hstmt)
	if err != nil {
		return nil, err
	}
	return &execResult{result: result}, nil
}

// QueryContext implements driver.StmtQueryContext.
func (s *preparedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	hstmt, err := buildStmt(s.sql, args, true)
	if err != nil {
		return nil, err
	}
	result, err := s.conn.execStatement(ctx, hstmt)
	if err != nil {
		return nil, err
	}
	return newRows(result), nil
}
