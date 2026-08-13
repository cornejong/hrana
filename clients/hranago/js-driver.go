//go:build js && wasm

package hranago

import (
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("hranajs", &JSDriver{})
}

// Driver implements database/sql/driver.Driver.
type JSDriver struct{}

func (d *JSDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}

	if cfg.transport == "ws" {
		return newJSWSConn(cfg)
	}

	return newConn(cfg), nil
}
