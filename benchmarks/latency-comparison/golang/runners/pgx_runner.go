/*
Copyright 2023 Google LLC
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package runners

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

func RunPgx(database, sql string, readWrite bool, numOperations, numClients, wait int, host string, port int, useUnixSocket bool) ([]float64, error) {
	ctx := context.Background()
	var err error

	// Connect to Cloud Spanner through PGAdapter.
	var connString string
	if useUnixSocket {
		connString = fmt.Sprintf("host=%s port=%d database=%s", host, port, database)
	} else {
		connString = fmt.Sprintf("postgres://uid:pwd@%s:%d/%s?sslmode=disable", host, port, url.QueryEscape(database))
	}
	conns := make([]*pgx.Conn, numClients)
	for c := 0; c < numClients; c++ {
		conns[c], err = pgx.Connect(ctx, connString)
		if err != nil {
			return nil, err
		}
		defer conns[c].Close(ctx)
	}

	// Run one query to warm up.
	if readWrite {
		if _, err := executePgxUpdate(ctx, conns[0], sql); err != nil {
			return nil, err
		}
	} else {
		if _, err := executePgxQuery(ctx, conns[0], sql); err != nil {
			return nil, err
		}
	}

	var ops atomic.Int64
	var finished atomic.Bool
	totalOps := numOperations * numClients
	runTimes := make([]float64, totalOps)
	wg := sync.WaitGroup{}
	wg.Add(numClients)
	for c := 0; c < numClients; c++ {
		clientIndex := c
		go func() error {
			defer wg.Done()
			for n := 0; n < numOperations; n++ {
				randWait(wait)
				if readWrite {
					runTimes[clientIndex*numOperations+n], err = executePgxUpdate(ctx, conns[clientIndex], sql)
				} else {
					runTimes[clientIndex*numOperations+n], err = executePgxQuery(ctx, conns[clientIndex], sql)
				}
				if err != nil {
					return err
				}
				ops.Add(1)
			}
			return nil
		}()
	}
	printProgress(&finished, &ops, totalOps)
	wg.Wait()
	finished.Store(true)
	fmt.Printf("\r%d/%d\n\n", ops.Load(), totalOps)
	return runTimes, nil
}

func executePgxQuery(ctx context.Context, conn *pgx.Conn, sql string) (float64, error) {
	start := time.Now()

	var res *string
	err := conn.QueryRow(ctx, sql, randId(100000)).Scan(&res)
	if err != nil && err != pgx.ErrNoRows {
		return 0, err
	}
	numNull := 0
	numNonNull := 0
	if res == nil {
		numNonNull++
	} else {
		numNull++
	}
	end := float64(time.Since(start).Microseconds()) / 1e3
	return end, nil
}

func executePgxUpdate(ctx context.Context, conn *pgx.Conn, sql string) (float64, error) {
	start := time.Now()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, sql, randString(), randId(100000)); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	end := float64(time.Since(start).Microseconds()) / 1e3
	return end, nil
}
