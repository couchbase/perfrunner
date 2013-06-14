from uuid import uuid4
from couchbase import Couchbase

cb = Couchbase.connect(bucket='benchmarks', **{'host': '127.0.0.1', 'port': 8091, 'username': 'Administrator', 'password': 'password'})


import random

b = 100
for _ in range(50):
    b += 1
    data = {'build': '2.0.4-{0}'.format(b), 'metric': 'compact_bucket_10M_vesta', 'value': random.randint(650, 750)}
    key = uuid4().hex
    cb.set(key, data)
