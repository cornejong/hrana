package main

import (
	"context"
	"database/sql"
	"fmt"
)

const createTableSQL = `
CREATE TABLE IF NOT EXISTS stress_kv (
    id  INTEGER PRIMARY KEY,
    val TEXT    NOT NULL
)`

func setupSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, createTableSQL)
	return err
}

// seedSchema inserts rows 1..total using batched transactions to avoid
// a single large transaction on slow servers.
func seedSchema(ctx context.Context, db *sql.DB, total int) error {
	const batchSize = 500
	for start := 1; start <= total; start += batchSize {
		end := start + batchSize - 1
		if end > total {
			end = total
		}
		if err := seedBatch(ctx, db, start, end); err != nil {
			return err
		}
	}
	return nil
}

func seedBatch(ctx context.Context, db *sql.DB, from, to int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO stress_kv(id, val) VALUES (?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()

	for i := from; i <= to; i++ {
		if _, err := stmt.ExecContext(ctx, i, fmt.Sprintf("seed-%d", i)); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func teardownSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS stress_kv`)
	return err
}

// needsSeed returns true for workloads that require pre-seeded rows to read.
func needsSeed(workload string) bool {
	switch workload {
	case "read", "mixed", "tx":
		return true
	}
	return false
}

// needsRows returns true for workloads that use --rows as an ID range.
func needsRows(workload string) bool {
	switch workload {
	case "read", "write", "mixed", "tx":
		return true
	}
	return false
}
