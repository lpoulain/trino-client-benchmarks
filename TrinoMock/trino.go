package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type Test struct {
	query        string
	nbIterations int
	nbThreads    int
}

var tests []Test
var queries map[string][]byte
var nextUris map[string]map[string][]byte
var sql map[string]string

func load_query(hash string) bool {
	nextUris[hash] = make(map[string][]byte)

	files, err := ioutil.ReadDir(fmt.Sprintf("./data/%s/", hash))
	if err != nil {
		return false
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if file.Name() == "query.json" {
			content, err := ioutil.ReadFile(fmt.Sprintf("./data/%s/query.json", hash))
			if err != nil {
				panic(err)
			}
			queries[hash] = content
			//			fmt.Printf("[%s] [%d] [%d]\n", details[0], nbIterations, nbThreads)

			//				fmt.Println(file.Name())
		} else {
			content, err := ioutil.ReadFile(fmt.Sprintf("./data/%s/%s", hash, file.Name()))
			if err != nil {
				panic(err)
			}
			idx := strings.Split(file.Name(), ".")[0]

			nextUris[hash][idx] = content
			//				fmt.Printf("%s (%s), %d bytes\n", file.Name(), idx, len(content))
		}
		//			f, err := os.Open(fmt.Sprintf("./data/%d/query.json", i))
		/*			defer f.Close()

					stats, statsErr := file.Stat()
					if statsErr != nil {
						return nil, statsErr
					}

					var size int64 = stats.Size()
					bytes := make([]byte, size)*/
	}

	return true
}

func main() {
	file, err := os.Open("../queries.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	tests = make([]Test, 100)
	queries = make(map[string][]byte)
	nextUris = make(map[string]map[string][]byte)
	sql = make(map[string]string)
	i := 0

	load_query("parquet")

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

		hash := get_hash(details[0])

		result := load_query(hash)

		if result {
			fmt.Printf("[%s] [%d] [%d]\n", details[0], nbIterations, nbThreads)
		}

		i += 1
		/*
			//		iStr := strconv.Itoa(i)
			//		sql[details[0]] = iStr
			nextUris[hash] = make(map[string][]byte)

			files, err := ioutil.ReadDir(fmt.Sprintf("./data/%s/", hash))
			if err != nil {
				//			log.Fatal(err)
				i += 1
				continue
			}

			for _, file := range files {
				if file.IsDir() {
					continue
				}

				if file.Name() == "query.json" {
					content, err := ioutil.ReadFile(fmt.Sprintf("./data/%s/query.json", hash))
					if err != nil {
						panic(err)
					}
					queries[hash] = content
					fmt.Printf("[%s] [%d] [%d]\n", details[0], nbIterations, nbThreads)

					//				fmt.Println(file.Name())
				} else {
					content, err := ioutil.ReadFile(fmt.Sprintf("./data/%s/%s", hash, file.Name()))
					if err != nil {
						panic(err)
					}
					idx := strings.Split(file.Name(), ".")[0]

					nextUris[hash][idx] = content
					//				fmt.Printf("%s (%s), %d bytes\n", file.Name(), idx, len(content))
				}
				//			f, err := os.Open(fmt.Sprintf("./data/%d/query.json", i))
			}
			i += 1
		*/
		//		if nbThreads > 1 {
		//			runMultithread("", details[0], nbThreads)
		//		} else {
		//			run("", details[0], nbIterations)
		//		}
	}

	http.HandleFunc("/", runTest)
	fmt.Println("Mock Trino Server starting on port 8081")
	http.ListenAndServe(":8081", nil)
}

func get_hash(sql string) string {
	h := sha256.New()
	h.Write([]byte(strings.Trim(sql, "\t \f \v \n")))
	return hex.EncodeToString(h.Sum(nil))[0:8]
}

func runTest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		body, _ := ioutil.ReadAll(r.Body)
		fmt.Printf("%s request: [%s]\n", r.Method, body)
		hash := get_hash(string(body))

		w.Header().Set("Content-Type", "application/json")
		w.Write(queries[hash])
		break
	case "GET":
		subPaths := strings.Split(r.URL.RequestURI(), "/")
		nbSubPaths := len(subPaths)
		queryId := subPaths[nbSubPaths-2]
		pageId := subPaths[nbSubPaths-1]

		//		fmt.Printf("%s request: %s (query %s, page %s)\n", r.Method, r.URL.RequestURI(), queryId, pageId)

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Vary", "Accept-Encoding, User-Agent")
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)
		w.Write(nextUris[queryId][pageId])
		break
	default:
		fmt.Printf("%s request: [%s] (no-op)\n", r.Method, r.URL.RequestURI())
	}
	/*	if r.Method == "POST" {
			body, _ := ioutil.ReadAll(r.Body)
			fmt.Printf("%s request: [%s]\n", r.Method, body)
			hash := get_hash(string(body))
			//		testIdx := sql[string(body)]
			//	var test = tests[2]
			w.Header().Set("Content-Type", "application/json")
			w.Write(queries[hash])
		} else {
			subPaths := strings.Split(r.URL.RequestURI(), "/")
			nbSubPaths := len(subPaths)
			queryId := subPaths[nbSubPaths-2]
			pageId := subPaths[nbSubPaths-1]

			fmt.Printf("%s request: %s (query %s, page %s)\n", r.Method, r.URL.RequestURI(), queryId, pageId)

			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("Vary", "Accept-Encoding, User-Agent")
			w.Header().Set("Content-Encoding", "gzip")
			w.WriteHeader(http.StatusOK)
			w.Write(nextUris[queryId][pageId])
		} else {

		}*/
}
