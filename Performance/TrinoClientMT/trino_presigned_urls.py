import boto3

client = boto3.client("s3",region_name='us-east-2')

urls = []

for i in range(1, 1175):
    response = client.generate_presigned_url('get_object', Params={'Bucket': 'alex2-galaxy', 'Key': 'test_trino/%d.json.gz' % (i,)}, ExpiresIn=600000)
    urls.append(response)

with open('urls.txt', 'w') as f:
    f.write('\n'.join(urls))
