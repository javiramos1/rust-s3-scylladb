import boto3
import requests
import os
import asyncio
import aiohttp
import time
import random

start_time = time.time()

url = os.environ['URL']
print("URL " + url)

bucket = os.environ['BUCKET']
print("BUCKET " + bucket)

headers = {'Content-Type': 'application/json'}
timeout = aiohttp.ClientTimeout(total=79900)

client = boto3.client(
        "s3"
    )

async def post_request(file):
    file = "s3://{0}/{1}".format(bucket, file)
    data = {
            "ingestion_id": "test",
            "files": [file]
           }
    print("Sending " + str(data))
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json = data, headers=headers, timeout=timeout) as resp:
            data = await resp.text()
            print ("Got Reply: " + data)

futures = []

index = 0
for key in client.list_objects(Bucket=bucket)['Contents']:
    print(key['Key'])
    file = key['Key']

    futures.append(post_request(file))
    if index >= 36:
        print("waiting...")
        index = 0
    index += 1

loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.wait(futures))
print("Took %s" % (time.time() - start_time))
print("Completed!")