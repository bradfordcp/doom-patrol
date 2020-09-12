import os
from astra import AstraClient
import json
import asyncio
import contextlib
import time

@contextlib.contextmanager
def report_time(test, iters):
    t0 = time.time()
    yield
    t1 = time.time()
    print("Total time needed for `%s': %.2fs" % (test, t1 - t0))
    print("Mean: %.2fs across %d" % ((t1 - t0) / float(iters), iters))

with AstraClient.new() as client:
    keyspace = 'dooooom'
    collection = 'widgets'

    document = {'a': 'a'}
    document_id = '00489f2c-5283-4fe8-a4c2-e8691982f917'
    doc_count = 10
    documents = [document for i in range(doc_count)]

    loop = asyncio.get_event_loop()

    path = f"/v2/namespaces/{keyspace}/collections/{collection}"

    with report_time("requests POST", doc_count):
        for document in documents:
            client.request('POST', path, json=document)

    with report_time("aiohttp POST", doc_count):
        reqs = []
        resps = []

        for document in documents:
            reqs.append(client.async_request('POST', path, json=document))
        
        res2 = loop.run_until_complete(asyncio.gather(*reqs))
    
    with report_time("requests GET", doc_count):
        for document in documents:
            client.request('GET', f"{path}/{document_id}").json()

    with report_time("aiohttp GET", doc_count):
        reqs = []
        resps = []

        for document in documents:
            reqs.append(client.async_request('GET', f"{path}/{document_id}"))
        
        res2 = loop.run_until_complete(asyncio.gather(*reqs))
