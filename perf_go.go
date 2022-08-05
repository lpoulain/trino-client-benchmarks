package main

import (
	"database/sql"
	"fmt"
	"time"
	"sync"
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/trinodb/trino-go-client/trino"
)

func main() {
	file, err := os.Open("queries.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		query := scanner.Text()
		if query == "" {
			continue
		}
		details := strings.Split(query, "|")
		if details[0] == "" {
			continue
		}

		nbThreads, _ := strconv.Atoi(details[2])
		nbIterations, _ := strconv.Atoi(details[1])

fmt.Sprintf("[%s] [%d] [%d]\n", details[0], nbIterations, nbThreads)

		if nbThreads > 1 {
			runMultithread("", details[0], nbThreads)
		} else {
			run("", details[0], nbIterations)
		}
	}
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
	fmt.Printf("[%s] x %4d: %9.6fs\n", query, iterations, elapsed.Seconds())
}

func runMultithread(testName string, query string, nbThreads int) {
        db, _ := sql.Open("trino", "http://go@localhost:8080")
        defer db.Close()

	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i<nbThreads; i++ {
		wg.Add(1);
		go func() {
			defer wg.Done()
			rows, _ := db.Query(string(query))
                	defer rows.Close()
                
        	        for rows.Next() {
	                }
		}()
	}

	wg.Wait()
        elapsed := time.Since(start)
        fmt.Printf("[%s] x %d threads: %fs\n", query, nbThreads, elapsed.Seconds())
}

