// Package hranago provides a database/sql driver for connecting to a remote
// SQLite database via the Hrana protocol.
//
// Register the driver by importing with a blank identifier:
//
//	import _ "github.com/cornejong/hrana/clients/hranago"
//
// Then open a connection using the "hrana" driver name. The DSN scheme
// determines the transport:
//
//	http://host:port?token=<authToken>           (HTTP pipeline, v3 default)
//	https://host:port?token=<authToken>&version=v2
//	ws://host:port?token=<authToken>             (WebSocket, v3 default)
//	wss://host:port?token=<authToken>&version=v1
package hranago

import (
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("hrana", &Driver{})
}

// Driver implements database/sql/driver.Driver.
type Driver struct{}

// Open parses the DSN and returns a new Conn. The scheme selects the transport:
// http/https uses the HTTP pipeline API; ws/wss uses the WebSocket API.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.transport == "ws" {
		return newWSConn(cfg)
	}
	return newConn(cfg), nil
}

func (d *Driver) Borrow() error {
	return nil
}

func (d *Driver) Return() error {
	return nil
}

func (d *Driver) Lock() error {
	return nil
}
func (d *Driver) UnLock() error {
	return nil
}
