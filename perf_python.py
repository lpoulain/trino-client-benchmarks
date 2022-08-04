from datetime import datetime
from threading import Thread

import trino
conn = trino.dbapi.connect(
    host='localhost',
    port=8080,
    user='sep-user',
    catalog='memory',
    schema='information_schema',
)

def run(test_name, sql, iterations):
    cur = conn.cursor()

    start = datetime.now()
    for i in range(iterations):
        cur.execute(sql)
        rows = cur.fetchall()

    end = datetime.now()
    duration = end - start

    print("[%s] x %4d: %d.%06ds" % (sql, iterations, duration.seconds, duration.microseconds))


def exec(sql):
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

def run_multithread(test_name, sql, nb_threads):
    threads = []

    for _ in range(nb_threads):
        threads.append(Thread(target=exec, args=(sql,)))

    start = datetime.now()

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    end = datetime.now()
    duration = end - start
    
    print("[%s] x %4d threads: %d.%06ds" % (sql, nb_threads, duration.seconds, duration.microseconds))


run("Small query", "SELECT 1                                    ", 1000)
run("Large query", "SELECT * FROM tpch.sf100.orders LIMIT 100000", 10)
run("Large query", "SELECT * FROM tpch.sf100.orders LIMIT 100000", 1)
run_multithread("Large query", "SELECT * FROM tpch.sf100.orders LIMIT 100000", 10)
