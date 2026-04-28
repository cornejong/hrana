package hranago

import "context"

// tx implements driver.Tx. It sends BEGIN/COMMIT/ROLLBACK as plain statements
// over the same stream (baton) the connection already has open.
type tx struct {
	conn stmtExecutor
	ctx  context.Context
}

// Commit sends COMMIT to the server.
func (t *tx) Commit() error {
	_, err := t.conn.execStatement(t.ctx, &stmt{SQL: "COMMIT", WantRows: false})
	return err
}

// Rollback sends ROLLBACK to the server.
func (t *tx) Rollback() error {
	_, err := t.conn.execStatement(t.ctx, &stmt{SQL: "ROLLBACK", WantRows: false})
	return err
}
