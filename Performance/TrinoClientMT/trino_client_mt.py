import json
import requests
from datetime import datetime
import threading
import gzip
import asyncio
from concurrent.futures import ThreadPoolExecutor
import requests.adapters
import urllib3
import aiohttp
import asyncio

results_start = None
results_cur = None

threadLock = threading.Lock()
global_counter = 0

first_results_ready = threading.Lock()
first_results_ready.acquire()

with open('urls.txt') as f:
    urls = [url.strip() for url in f.readlines()]


class Results:
    def __init__(self, prev):
        self.id = 1 if prev is None else prev.id + 1
        self.next = None
        self.prev = prev
        if prev:
            prev.next = self
        self.data = []
        self.idx = 0
#        self.next_results_ready = asyncio.Event()
#        self.next_results_ready.clear()
        self.next_results_ready = threading.Lock()
        self.next_results_ready.acquire()

    def next_value(self):
        idx = self.idx
        if idx >= len(self.data):
            return None
        self.idx += 1
        return self.data[idx]

#    async def next_results(self):
#        if self.next is None:
#            return None
#        await self.next_results_ready.wait()
#        return self.next


def parse_body(url, results):
    global first_results_ready

    try:
#        print('<- [' + url[:60] + ']')
#        response = requests.get(url, stream=True)
#        start = datetime.now()
        response = http.request('GET', url)
#        end = datetime.now()
#        duration = end - start
#        print("GET: %d.%06ds" % (duration.seconds, duration.microseconds))

#        if 'Content-Encoding' in response.headers and response.headers['Content-Encoding'] == 'gzip':
        body = gzip.decompress(response.data).decode('utf-8')
#        else:
#        if response.status_code != 200:
#            print(response.status_code)
#            print(url)
#            return
#        body = response.content.decode('utf-8')
#        print(len(body))

        j = json.loads(body)
        if 'data' in j:
            results.data = j['data']
#            print(url[:60] + ' completed (' + str(len(j['data'])) + ')')
        else:
            results.data = []

        if results.prev:
#            print('Unlocking %d' % (results.prev.id,))
            results.prev.next_results_ready.release()
        else:
#            print('Unlocking first event')
            first_results_ready.release()

    except Exception as e:
        print('ERROR', response.data)
        print(e)
#        print(response.url + ' completed')


#    if 'X-Trino-Nexturi' not in response.headers:
#        self.next_results_ready.release()

#    with threadLock:
#        global_counter += 1


def follow_trail(response):
    global results_cur
    global results_start

    threads = []
    start = datetime.now()

    with ThreadPoolExecutor(max_workers=16) as executor:

        while True:
            if results_cur is None:
                results_cur = Results(None)
                results_start = results_cur
            else:
                results_cur = Results(results_cur)

    #        thread = threading.Thread(target=parse_body, args=(response, results_cur))
    #        threads.append(thread)
    #        thread.start()
            executor.submit(parse_body, response, results_cur)

            if 'X-Trino-Nexturi' not in response.headers:
                results_cur.next_results_ready.release()
                break

            uri = response.headers['X-Trino-Nexturi']
    #        print(uri)
            response = requests.head(uri)

#    print(str(len(threads)) + ' threads')
#    for thread in threads:
#        thread.join()

    end = datetime.now()
    duration = end - start

    print("%d nextUris: %d.%06ds" % (len(threads), duration.seconds, duration.microseconds))

http = urllib3.PoolManager(num_pools=8)
executor = ThreadPoolExecutor(max_workers=8)

async def read_all_urls_async():
    global results_cur
    global results_start
    global first_results_ready

    counter = 0
    start = datetime.now()

#    async with aiohttp.ClientSession() as session:
    for url in urls:
        response = http.request('HEAD', 'https://insights.dogfood.eng.starburstdata.net/ui/insights/login')
        print(url[:60])
#        if results_cur is None:
#            results_cur = Results(None)
#            results_start = results_cur
#        else:
#            results_cur = Results(results_cur)

    """
            async with session.get(url) as resp:
#                print(dir(resp))
                txt = await resp.read()
                body = gzip.decompress(txt).decode('utf-8')
                j = json.loads(body)

                if 'data' in j:
                    results_cur.data = j['data']
                    print(url[:60] + ' completed (' + str(len(j['data'])) + ')')
                else:
                    results_cur.data = []

                if results_cur.prev:
                    print('Unlocking %d' % (results_cur.prev.id,))
                    results_cur.prev.next_results_ready.release()
                else:
                    print('Unlocking first event')
                    first_results_ready.release()
            counter += 1
"""

    results_cur.next_results_ready.release()

    end = datetime.now()
    duration = end - start

    print("%d nextUris from S3: %d.%06ds" % (counter, duration.seconds, duration.microseconds))


def read_all_urls():
    global results_cur
    global results_start

    counter = 0
    start = datetime.now()

#    with ThreadPoolExecutor(max_workers=64) as executor:
    for url in urls:
    #        response = requests.head(url)
#        start = datetime.now()
        response = http.request('HEAD', url)
#        end = datetime.now()
#        duration = end - start
#        print("HEAD: %d.%06ds" % (duration.seconds, duration.microseconds))
        if results_cur is None:
            results_cur = Results(None)
            results_start = results_cur
        else:
            results_cur = Results(results_cur)

#            print('Launching ' + url[:60])
        executor.submit(parse_body, url, results_cur)
#        parse_body(url, results_cur)
        counter += 1

    results_cur.next_results_ready.release()

    end = datetime.now()
    duration = end - start

    print("%d nextUris from S3: %d.%06ds" % (counter, duration.seconds, duration.microseconds))



async def next():
    print('Wait for first results...')
    first_results_ready.acquire()
    print('First results ready')

    results_cur = results_start

    while True:
        value = results_cur.next_value()

        while value is None:
#            print('Wait for result %d... (locked=%s)' % (results_cur.id, str(results_cur.next_results_ready.locked())))
            results_cur.next_results_ready.acquire()
#            print('Result %d ready' % (results_cur.id,))
            if results_cur.next is None:
                return
            results_cur = results_cur.next
#            results_cur = results_cur.next_results()
            value = results_cur.next_value()

        yield value


def launch_query(sql):
    global executor

    start = datetime.now()

    if sql == '':
#        executor.submit(read_all_urls)
        asyncio.run(read_all_urls_async())
#        thread = threading.Thread(target=read_all_urls, daemon=True)
#        thread.start()
    else:
        headers = {'X-Trino-User': 'user'}
        response = requests.post('http://localhost:8081/v1/statement', headers=headers, data = sql, stream=True)
        body = response.content.decode('utf-8')
        thread = threading.Thread(target=follow_trail, args=(response, ))
        thread.start()

async def read_results():
    global start
    counter = 0
    async for val in next():
        counter += 1
        if counter % 100000 == 0:
            print('%d rows' % (counter,))

    end = datetime.now()
    duration = end - start

    print("%d rows: %d.%06ds" % (counter, duration.seconds, duration.microseconds))


start = datetime.now()

#launch_query('SELECT * FROM tpch.sf100.orders LIMIT 10000000')
launch_query('')
asyncio.run(read_results())
