package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/trinodb/trino-go-client/trino"
)

func main() {
	run("Small query", "SELECT 1                                    ", 1000);
	run("Large query", "SELECT * FROM tpch.sf100.orders LIMIT 100000", 10);
}

func run(testName string, query string, iterations int) {
	db, _ := sql.Open("trino", "http://go@localhost:8080")
	defer db.Close()

        start := time.Now()

        for i := 0; i<iterations; i++ {
 		rows, _ := db.Query(string(query))
		defer rows.Close()

		for rows.Next() {
		}
        }

        elapsed := time.Since(start)
	fmt.Printf("[%s] x %4d: %s\n", query, iterations, elapsed)
}
