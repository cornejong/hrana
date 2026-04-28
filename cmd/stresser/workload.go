package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
)

type workloadCfg struct {
	rows      int
	readRatio float64
}

type workloadFn func(ctx context.Context, conn *sql.Conn, rng *rand.Rand, cfg workloadCfg) error

func resolveWorkload(name string) (workloadFn, error) {
	switch name {
	case "ping":
		return workloadPing, nil
	case "read":
		return workloadRead, nil
	case "write":
		return workloadWrite, nil
	case "mixed":
		return workloadMixed, nil
	case "tx":
		return workloadTx, nil
	default:
		return nil, fmt.Errorf("unknown workload %q; valid: ping, read, write, mixed, tx", name)
	}
}

func workloadPing(ctx context.Context, conn *sql.Conn, _ *rand.Rand, _ workloadCfg) error {
	rows, err := conn.QueryContext(ctx, "SELECT 1")
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	return rows.Close()
}

func workloadRead(ctx context.Context, conn *sql.Conn, rng *rand.Rand, cfg workloadCfg) error {
	id := rng.IntN(cfg.rows) + 1
	rows, err := conn.QueryContext(ctx, "SELECT val FROM stress_kv WHERE id = ?", id)
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	return rows.Close()
}

func workloadWrite(ctx context.Context, conn *sql.Conn, rng *rand.Rand, cfg workloadCfg) error {
	id := rng.IntN(cfg.rows) + 1
	val := randString(rng, 16)
	_, err := conn.ExecContext(ctx,
		`INSERT INTO stress_kv(id, val) VALUES (?, ?)
		 ON CONFLICT(id) DO UPDATE SET val = excluded.val`,
		id, val,
	)
	return err
}

func workloadMixed(ctx context.Context, conn *sql.Conn, rng *rand.Rand, cfg workloadCfg) error {
	if rng.Float64() < cfg.readRatio {
		return workloadRead(ctx, conn, rng, cfg)
	}
	return workloadWrite(ctx, conn, rng, cfg)
}

func workloadTx(ctx context.Context, conn *sql.Conn, rng *rand.Rand, cfg workloadCfg) error {
	id := rng.IntN(cfg.rows) + 1
	val := randString(rng, 16)

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, "UPDATE stress_kv SET val = ? WHERE id = ?", val, id); err != nil {
		_ = tx.Rollback()
		return err
	}

	rows, err := tx.QueryContext(ctx, "SELECT val FROM stress_kv WHERE id = ?", id)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	for rows.Next() {
	}
	if err := rows.Close(); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

const randChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randString(rng *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = randChars[rng.IntN(len(randChars))]
	}
	return string(b)
}
