package hranago

import (
	"database/sql/driver"
	"io"
	"reflect"
)

// rows implements driver.Rows and driver.RowsColumnTypeScanType.
type rows struct {
	result *stmtResult
	pos    int
}

func newRows(r *stmtResult) *rows {
	return &rows{result: r}
}

// Columns returns the column names from the result set.
func (r *rows) Columns() []string {
	names := make([]string, len(r.result.Cols))
	for i, c := range r.result.Cols {
		if c.Name != nil {
			names[i] = *c.Name
		}
	}
	return names
}

// Close is a no-op; the result set is already buffered in memory.
func (r *rows) Close() error { return nil }

// Next copies the next row into dest. Returns io.EOF when exhausted.
func (r *rows) Next(dest []driver.Value) error {
	if r.pos >= len(r.result.Rows) {
		return io.EOF
	}
	row := r.result.Rows[r.pos]
	r.pos++
	for i, v := range row {
		dest[i] = hranaValueToDriver(v)
	}
	return nil
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType.
// Returns the most specific Go type for each column based on the declared type.
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	if index >= len(r.result.Cols) {
		return reflect.TypeOf((*any)(nil)).Elem()
	}
	c := r.result.Cols[index]
	if c.Decltype == nil {
		return reflect.TypeOf((*any)(nil)).Elem()
	}
	switch *c.Decltype {
	case "INTEGER", "INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT":
		return reflect.TypeOf(int64(0))
	case "REAL", "FLOAT", "DOUBLE":
		return reflect.TypeOf(float64(0))
	case "TEXT", "VARCHAR", "CHAR", "CLOB":
		return reflect.TypeOf("")
	case "BLOB":
		return reflect.TypeOf([]byte{})
	default:
		return reflect.TypeOf((*any)(nil)).Elem()
	}
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName.
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	if index >= len(r.result.Cols) {
		return ""
	}
	c := r.result.Cols[index]
	if c.Decltype == nil {
		return ""
	}
	return *c.Decltype
}
