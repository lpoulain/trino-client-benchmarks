# Trino Client Benchmarks

This repo is helping testing various Trino Clients performance (Java/JDBC, Python client library, Go client library). It requires a Trino server running locally on port 8080 without any authentication.

## Tests

The tests running are described in `queries.txt` are describe:

- The SQL command to execute
- The number of iterations
- The number of threads

## Executing the clients

You can build and/or execute each client the following way:

    $ python3 perf_python.py
    $ 
    $ export CLASSPATH=.:trino-jdbc-387.jar
    $ javac PerfJDBC.java
    $ java PerfJDBC
    $
    $ go build
    $ ./perf_go

Each client will go through `queries.txt` and execute each test, displaying the time spent.

Each client can also connect to a Trino server on port 8081 (see "Mock Trino Server" below):

    $ python3 perf_python.py --mock
    $ java PerfJDBC --mock
    $ ./perf_go --mock

## Mock Trino Server

The `TrinoMock/trino_mock` executable (written in Go, the Java version doesn't work yet) allows to mock a Trino server:

    $ cd TrinoMock
    $ python3 extract_queries.py
    $ go build
    $ ./trino_mock

The `extract_queries.py` Python script calls a local Trino server on port 8080 (no auth), executes the queries and stores the HTTP responses in `./TrinoMock/data/`

When `./trino_mock` is executed, a mock Trino server runs on port 8081. At startup it goes through `../queries.txt` and for each SQL query loads the data in memory, providing very fast responses.

## Running as a service

Passing `--server` runs each client as a server, which can then be called by a tool such as JMeter.
