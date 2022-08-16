import os
import hashlib
import requests
import json
import gzip

def get_hash(sql):
    m = hashlib.sha256(str.encode(sql)).hexdigest()
    return m[:8]

def extract(sql):
    hash = get_hash(sql.strip())
    if os.path.exists('./data/' + hash):
        print('[{}] - {} - Skipping'.format(sql, hash))
        return
    print('[{}] - {}'.format(sql, hash))

    os.mkdir('./data/' + hash)
    headers = {'X-Trino-User': 'user'}
    response = requests.post('http://localhost:8080/v1/statement', headers=headers, data = sql)
    body = response.content.decode('utf-8')
#    print(body)
    j = json.loads(body)
    next_uri = j['nextUri']
    body = body.replace(next_uri, 'http://localhost:8081/ui/query.html?00000000_000000_00000_xxxxx/{}/1'.format(hash))

    path = './data/{}/query.json'.format(hash)
    with open(path, 'w') as file:
        print(path)
        file.write(body)

    i = 1

    while next_uri:
        response = requests.get(next_uri)

        body = response.content.decode('utf-8')
#        print(body)
        j = json.loads(body)
        if 'nextUri' in j:
            next_uri = j['nextUri']
            body = body.replace(next_uri, 'http://localhost:8081/ui/query.html?00000000_000000_00000_xxxxx/{}/{}'.format(hash, i+1))
            g = gzip.compress(body.encode())
        else:
            g = gzip.compress(response.content)
            next_uri = None

        path = './data/{}/{}.json.gz'.format(hash, i)
        with open(path, 'wb') as file:
            print(path)
            file.write(g)
        
        i += 1

with open('../queries.txt') as f:
    for query in f.readlines():
        details = query.strip().split('|')
        if details == '' or details[0] == '':
            continue

        sql = details[0]
        r = extract(sql)
