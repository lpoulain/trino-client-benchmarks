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
    cur = conn.cursor(experimental_python_types=True)

    start = datetime.now()
    for i in range(iterations):
        cur.execute(sql)
        rows = cur.fetchall()

    end = datetime.now()
    duration = end - start

    print("[%s] x %4d: %2d.%06ds" % (sql, iterations, duration.seconds, duration.microseconds))


def exec(sql, i):
    cur = conn.cursor(experimental_python_types=True)
    cur.execute(sql)
    rows = cur.fetchall()

def run_multithread(test_name, sql, nb_threads):
    threads = []

    i = 0
    for _ in range(nb_threads):
        threads.append(Thread(target=exec, args=(sql,i,)))
        i += 1

    start = datetime.now()

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    end = datetime.now()
    duration = end - start
    
    print("[%s] x %d threads: %d.%06ds" % (sql, nb_threads, duration.seconds, duration.microseconds))


with open('queries.txt') as f:
    for query in f.readlines():
        details = query.strip().split('|')
        if details == '' or details[0] == '':
            continue
        if int(details[2]) > 1:
            run_multithread('', details[0], int(details[2]))
        else:
            run("", details[0], int(details[1]))
