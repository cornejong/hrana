package hranago

import "database/sql/driver"

// Compile-time assertions that the concrete types satisfy the expected driver interfaces.
var (
	_ driver.Driver           = (*Driver)(nil)
	_ driver.Conn             = (*conn)(nil)
	_ driver.ConnBeginTx      = (*conn)(nil)
	_ driver.ExecerContext    = (*conn)(nil)
	_ driver.QueryerContext   = (*conn)(nil)
	_ driver.Conn             = (*wsConn)(nil)
	_ driver.ConnBeginTx      = (*wsConn)(nil)
	_ driver.ExecerContext    = (*wsConn)(nil)
	_ driver.QueryerContext   = (*wsConn)(nil)
	_ stmtExecutor            = (*conn)(nil)
	_ stmtExecutor            = (*wsConn)(nil)
	_ driver.Stmt             = (*preparedStmt)(nil)
	_ driver.StmtExecContext  = (*preparedStmt)(nil)
	_ driver.StmtQueryContext = (*preparedStmt)(nil)
	_ driver.Rows             = (*rows)(nil)
	_ driver.Tx               = (*tx)(nil)
	_ driver.Result           = (*execResult)(nil)
)
