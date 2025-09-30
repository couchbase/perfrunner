"""Create the wiki data set.

Steps:
1. download file : http://people.apache.org/~mikemccand/enwiki-20120502-lines-1k.txt.lzma
2. split on 2 parts
3. run the Docgen.start_load()
"""

import concurrent.futures
import datetime
import itertools
import random

from couchbase.bucket import Bucket


class Docgen:
    def __init__(self, master_file_path, shadow_file_path, cb_url, bucket_name, chunk_size=1024):
        self.chunk_size = chunk_size
        self.master_file_path = master_file_path
        self.shadow_file_path = shadow_file_path
        self.cb_url = cb_url
        self.bucket_name = bucket_name
        self.items = 1000000

    def read_file_gen(self, file):
        while True:
            data = file.read(self.chunk_size)
            if not data:
                break
            yield data

    def insert_cb(self, cb, master_file, shadow_file):
        master_lines = self.read_file_gen(master_file)
        shadow_lines = self.read_file_gen(shadow_file)
        counter = 0
        while True:
            test = {}
            text = master_lines.next().replace('\n', ' ')
            text2 = shadow_lines.next().replace('\n', ' ')
            test['text'] = text
            test['text2'] = text2
            key = hex(counter)[2:]

            try:
                cb.upsert(key, test)
                doc = cb.get(key).value

                if doc['text'] and doc['text2']:
                    if doc['text'] == text and doc['text2'] == text2:
                        counter += 1
            except Exception:
                pass

            if counter >= self.items:
                break

    def start_load(self):
        cb = Bucket("couchbase://{}/{}?operation_timeout=10".format(self.cb_url, self.bucket_name),
                    password="password")
        master_file = open(self.master_file_path)
        shadow_file = open(self.shadow_file_path)

        self.insert_cb(cb, master_file, shadow_file)
        master_file.close()
        shadow_file.close()


class Numeric(Docgen):
    def __init__(self, master_file_path, shadow_file_path, cb_url, bucket_name):
        super(Numeric, self).__init__(master_file_path, shadow_file_path, cb_url, bucket_name)
        '''
        now = datetime.datetime(2003, 6, 21, 10, 24, 23, 483163)
        '''
        self.start_yr = 2011
        self.count = 0
        self.month = itertools.cycle(range(1, 12))
        self.days = itertools.cycle(range(1, 28))
        self.hours = itertools.cycle(range(24))
        self.minutes = itertools.cycle(range(60))
        self.seconds = itertools.cycle(range(60))
        self.milis = itertools.cycle(range(48000, 483163))
        self.ranges = 3273681
        self.timenow = None
        self.numeric_ranges = []

    def form_date(self):
        # 1636840, 1091227, 818420 are divisible of self.ranges
        if self.count in [1636840, 1091227, 818420]:
            self.start_yr += 1
        self.count += 1
        return (
            self.start_yr,
            next(self.month),
            next(self.days),
            next(self.hours),
            next(self.minutes),
            next(self.seconds),
            next(self.milis)
        )

    def time_milis(self):
        self.timenow = datetime.datetime(*self.form_date())
        return int(self.timenow.strftime("%s")) * 1000

    def create_document(self):
        tmpcount = 0
        for value in self.numeric_ranges:
            if tmpcount % 3 == 0:
                print(value, ' max')
            elif tmpcount % 3 == 1:
                print(value, ' min')
            elif tmpcount % 3 == 2:
                upper = value << 1
                lower = value >> 1
                print(upper, ':', lower, ' max_min')
            tmpcount += 1

    def insert_cb(self, cb, *args):
        tmpcount = 0
        for r in range(self.ranges):
            key = hex(r)[2:]
            val = self.time_milis()
            r = random.randint(0, self.ranges)
            if r > 30000 and tmpcount < 10000:
                self.numeric_ranges.append(val)
                tmpcount += 1
            cb.upsert(key, {"time": val})

    def start_load(self):
        c = Bucket("couchbase://{}/{}?operation_timeout=10".format(self.cb_url, self.bucket_name),
                   password="password")
        self.insert_cb(c)


class Datefacet:
    def __init__(self):
        from multiprocessing import Lock, Manager
        self.cb = Bucket('couchbase://172.23.99.211/bucket-1', password="password")
        self.lock = Lock()
        self.dsize = 1000000
        self.dateiter = Manager().dict(
            {key: None for key in ['2013-10-17', '2013-11-17', '2014-02-09', '2015-11-26']}
        )

        self.dateiter['2013-10-17'] = .65 * self.dsize
        self.dateiter['2013-11-17'] = .2 * self.dsize
        self.dateiter['2014-02-09'] = .1 * self.dsize
        self.dateiter['2015-11-26'] = .05 * self.dsize
        self.cycledates = itertools.cycle(self.dateiter.keys())

    def createdateset(self):
        for resultid in range(0, self.dsize):
            key = hex(resultid)[2:]
            '''
            Day 1 should have approximately 65% of the documents
            Day 2 should have approximately 20% of the documents
            Day 3 should have approximately 10% of the documents
            Day 4 should have approximately 5% of the documents
            format like this 2010-07-27
            '''
            val = self.cb.get(key).value
            self.lock.acquire()
            tmpdate = next(self.cycledates)
            val["date"] = tmpdate
            self.cb.set(key, val)
            self.dateiter[tmpdate] -= 1
            if self.dateiter[tmpdate] == 0:
                self.dateiter.pop(tmpdate, None)
                self.cycledates = itertools.cycle(self.dateiter.keys())
            self.lock.release()

    def run(self):
        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
            executor.submit(self.createdateset())


'''
Usage templates:
A = Numeric('xaa','xbb', '172.23.99.211', 'bucket-1')
A.start_load()
A.create_document()

A = Docgen('xaa','xab','172.23.99.49','bucket-1')
A.start_load()

A = Datefacet()
A.run()
'''
