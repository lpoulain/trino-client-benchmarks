from datetime import datetime

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


run("Small query", "SELECT 1                                    ", 1000)
run("Large query", "SELECT * FROM tpch.sf100.orders LIMIT 100000", 10)

