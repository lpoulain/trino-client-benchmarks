package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/trinodb/trino-go-client/trino"
)

type Test struct {
	query        string
	nbIterations int
	nbThreads    int
}

var tests []Test
var db0 *sql.DB
var db1 *sql.DB

func main() {
	file, err := os.Open("queries.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	tests = make([]Test, 100)
	i := 0

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

		tests[i] = Test{query: details[0], nbIterations: nbIterations, nbThreads: nbThreads}
		i += 1

		//		fmt.Printf("[%s] [%d] [%d]\n", details[0], nbIterations, nbThreads)
	}

	db0, _ = sql.Open("trino", "http://go@localhost:8080")
	db1, _ = sql.Open("trino", "http://go@localhost:8081")
	//	defer db.Close()

	server := flag.Bool("server", false, "Run as a server")
	mock := flag.Bool("mock", false, "Using a Mock Trino server (on port 8081)")
	flag.Parse()

	if *server {
		http.HandleFunc("/", runTest)
		fmt.Println("Go client starting on port 3002")
		http.ListenAndServe(":3002", nil)
	} else {
		runAllTests(*mock)
	}
}

func runAllTests(mock bool) {
	db := db0
	if mock {
		db = db1
	}

	for _, test := range tests {
		if test.query == "" {
			continue
		}
		if test.nbThreads > 1 {
			runMultithread(db, "", test.query, test.nbThreads)
		} else {
			run(db, "", test.query, test.nbIterations)
		}
	}
}

func runTest(w http.ResponseWriter, r *http.Request) {
	testNb, err := strconv.Atoi(r.URL.Query().Get("test"))
	if err != nil {
		testNb = 2
	}
	var test = tests[testNb]
	db := db0
	trino := r.URL.Query().Get("trino")
	if trino == "mock" {
		db = db1
	} else {

	}

	if test.nbThreads > 1 {
		runMultithread(db, "", test.query, test.nbThreads)
	} else {
		run(db, "", test.query, test.nbIterations)
	}
}

func run(db *sql.DB, testName string, query string, iterations int) {
	start := time.Now()

	for i := 0; i < iterations; i++ {
		rows, err := db.Query(string(query))
		if err != nil {
			fmt.Printf("Error in the query: " + err.Error())
			return
		}
		defer rows.Close()

		for rows.Next() {
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("[%s] x %4d: %9.6fs\n", query, iterations, elapsed.Seconds())
}

func runMultithread(db *sql.DB, testName string, query string, nbThreads int) {
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < nbThreads; i++ {
		wg.Add(1)
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
