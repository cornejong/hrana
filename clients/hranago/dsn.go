package hranago

import (
	"fmt"
	"net/url"
	"strings"
)

type config struct {
	baseURL    string
	authToken  string
	apiVersion string // "v1", "v2", or "v3"
	transport  string // "http" or "ws"
}

// parseDSN parses a Hrana DSN URL into a config.
//
// Schemes:
//   - http / https → HTTP pipeline transport  (version: v2 or v3, default v3)
//   - ws  / wss   → WebSocket transport       (version: v1, v2, or v3, default v3)
//
// An optional "token" query parameter is used for authentication.
//
// Examples:
//
//	http://localhost:8080?token=secret
//	https://my-db.example.com?token=secret&version=v2
//	ws://localhost:8080?token=secret&version=v3
//	wss://my-db.example.com?token=secret
func parseDSN(dsn string) (*config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("hrana: invalid DSN %q: %w", dsn, err)
	}

	var transport string
	switch u.Scheme {
	case "http", "https":
		transport = "http"
	case "ws", "wss":
		transport = "ws"
	default:
		return nil, fmt.Errorf("hrana: DSN scheme must be http, https, ws, or wss, got %q", u.Scheme)
	}

	q := u.Query()
	token := q.Get("token")
	version := q.Get("version")
	if version == "" {
		version = "v3"
	}

	if transport == "http" {
		if version != "v2" && version != "v3" {
			return nil, fmt.Errorf("hrana: HTTP transport supports v2 or v3, got %q", version)
		}
	} else {
		if version != "v1" && version != "v2" && version != "v3" {
			return nil, fmt.Errorf("hrana: WebSocket transport supports v1, v2, or v3, got %q", version)
		}
	}

	u.RawQuery = ""
	baseURL := strings.TrimRight(u.String(), "/")

	return &config{
		baseURL:    baseURL,
		authToken:  token,
		apiVersion: version,
		transport:  transport,
	}, nil
}
