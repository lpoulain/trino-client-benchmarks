from http.server import HTTPServer, BaseHTTPRequestHandler
import argparse
from datetime import datetime
from threading import Thread
from urllib.parse import urlparse, parse_qs

import trino
conn0 = trino.dbapi.connect(
    host='localhost',
    port=8080,
    user='sep-user',
    catalog='memory',
    schema='information_schema',
)

conn1 = trino.dbapi.connect(
    host='localhost',
    port=8081,
    user='sep-user',
    catalog='memory',
    schema='information_schema',
)

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        query_string = urlparse(self.path)
        params = parse_qs(query_string.query)
        trino = params['trino'][0] if 'trino' in params else ''
        test_nb = int(params['test'][0]) if 'test' in params else 2

        conn = conn1 if trino == 'mock' else conn0
        test = tests[test_nb]
        if test.nb_threads > 1:
            run_multithread(conn, '', test.sql, test.nb_threads)
        else:
            run(conn, '', test.sql, test.nb_iterations)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(bytes("", "utf-8"))

    def log_message(self, format, *args):
        return

def run(conn, test_name, sql, iterations):
    cur = conn.cursor(experimental_python_types=True)

    start = datetime.now()
    for i in range(iterations):
        cur.execute(sql)
        rows = cur.fetchall()

    end = datetime.now()
    duration = end - start

    print("[%s] x %4d: %2d.%06ds" % (sql, iterations, duration.seconds, duration.microseconds))


def exec(conn, sql, i):
    cur = conn.cursor(experimental_python_types=True)
    cur.execute(sql)
    rows = cur.fetchall()

def run_multithread(conn, test_name, sql, nb_threads):
    threads = []

    i = 0
    for _ in range(nb_threads):
        threads.append(Thread(target=exec, args=(conn, sql,i,)))
        i += 1

    start = datetime.now()

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    end = datetime.now()
    duration = end - start
    
    print("[%s] x %d threads: %d.%06ds" % (sql, nb_threads, duration.seconds, duration.microseconds))


class Test:
    def __init__(self, sql, nb_iterations, nb_threads):
        self.sql = sql
        self.nb_iterations = nb_iterations
        self.nb_threads = nb_threads

tests = []

with open('queries.txt') as f:
    for query in f.readlines():
        details = query.strip().split('|')
        if details == '' or details[0] == '':
            continue

        test = Test(details[0], int(details[1]), int(details[2]))
        tests.append(test)
#        print("[{0}] [{1}] [{2}]".format(test.sql, test.nb_iterations, test.nb_threads))


parser = argparse.ArgumentParser()
parser.add_argument('--server', action=argparse.BooleanOptionalAction)
parser.add_argument('--mock', action=argparse.BooleanOptionalAction)
args = parser.parse_args()

if args.server:
    PORT = 3001

    with HTTPServer(("", PORT), Handler) as httpd:
        print("Python client starting at port", PORT)
        httpd.serve_forever()
else:
    conn = conn1 if args.mock else conn0

    for test in tests:
        if test.nb_threads > 1:
            run_multithread(conn, '', test.sql, test.nb_threads)
        else:
            run(conn, '', test.sql, test.nb_iterations)
