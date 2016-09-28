"""
This file is needed only for future reference to
load data from wiki data.
DOwnload link : http://people.apache.org/~mikemccand/enwiki-20120502-lines-1k.txt.lzma
"""
import datetime
import glob
import itertools
import os
import random

from couchbase.bucket import Bucket


class Docgen(object):
    def __init__(self, location, cb_url, database, chunk_size=1024):
        self.chunk_size = chunk_size
        self.location = location
        self.cb_url = cb_url
        self.database = database
        self.counts = 0
        '''
        The keygen part to create keys to optimize space
        '''
        self.key1 = self.keygen(2)
        self.key2 = self.keygen(4)
        self.key3 = self.keygen(5)

    def keygen(self, keynum):
        '''
        the ranges includes all ascii characters from 47 - 123

        '''
        s = list(map(chr, range(47, 123)))
        random.shuffle(s)
        iters = itertools.permutations(s, keynum)
        return iters

    def read_in_chunks(self, file_object):
        """Lazy function (generator) to read a file piece by piece.
        Default chunk size: 1k."""
        while True:
            data = file_object.read(self.chunk_size)
            if not data:
                break
            yield data

    def insert_cb(self, cb, f):
        for piece in self.read_in_chunks(f):
            test = {}
            piece = piece.replace('\n', ' ')
            test['text'] = piece
            key = ""
            if self.counts < 1000:
                key = self.key1.next()
            elif self.counts < 100000:
                key = self.key2.next()
            else:
                key = self.key3.next()
            key = ''.join(key)
            cb.upsert(key, test)

            if self.counts == 1000000:
                return
            self.counts += 1

    def start_load(self):
        os.chdir(self.location)
        c = Bucket("couchbase://{}/{}?operation_timeout=10".format(self.cb_url, self.database))
        for file in glob.glob("*.txt"):
            try:
                f = open(file)
                self.insert_cb(c, f)
                f.close()
            except Exception as e:
                print(self.counts, file)
                raise e


class Numeric(Docgen):
    def __init__(self, location, cb_url, database):
        super(Numeric, self).__init__(location, cb_url, database)
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
        '''

          1636840, 1091227, 818420 are divisble of self.ranges
        '''
        if self.count in [1636840, 1091227, 818420]:
            self.start_yr += 1
        self.count += 1
        return (
            self.start_yr,
            self.month.next(),
            self.days.next(),
            self.hours.next(),
            self.minutes.next(),
            self.seconds.next(),
            self.milis.next()
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

    def insert_cb(self, cb):
        tmpcount = 0
        for r in range(self.ranges):
            if r < 1000:
                key = self.key1.next()
            elif r < 100000:
                key = self.key2.next()
            else:
                key = self.key3.next()
            key = ''.join(key)
            val = self.time_milis()
            r = random.randint(0, self.ranges)
            if r > 30000 and tmpcount < 10000:
                '''
                 Doing random value collect,
                 numbers mentioned ar completely random
                 '''
                self.numeric_ranges.append(val)
                tmpcount += 1
            cb.upsert(key, {"time": val})

    def start_load(self):
        c = Bucket("couchbase://{}/{}?operation_timeout=30".format(self.cb_url, self.database))
        self.insert_cb(c)


class Datefacet:

    def __init__(self):
        from couchbase.n1ql import N1QLQuery
        from multiprocessing import Manager, Lock
        self.cb = Bucket('couchbase://172.23.123.38/bucket-1')
        self.row_iter = self.cb.n1ql_query(N1QLQuery('select meta().id from `bucket-1`'))
        self.lock = Lock()
        self.dsize = 1000000
        self.dateiter = Manager().dict({key: None for key in ['2013-10-17', '2013-11-17', '2014-02-09', '2015-11-26']})
        self.dateiter['2013-10-17'] = .65 * self.dsize
        self.dateiter['2013-11-17'] = .2 * self.dsize
        self.dateiter['2014-02-09'] = .1 * self.dsize
        self.dateiter['2015-11-26'] = .05 * self.dsize
        self.cycledates = itertools.cycle(self.dateiter.keys())

    def createdateset(self):
        for resultid in self.row_iter:
            '''
            Day 1 should have approximately 65% of the documents
            Day 2 should have approximately 20% of the documents
            Day 3 should have approximately 10% of the documents
            Day 4 should have approximately 5% of the documents
            format like this 2010-07-27
            '''
            val = self.cb.get(resultid["id"]).value
            self.lock.acquire()
            tmpdate = self.cycledates.next()
            val["date"] = tmpdate
            self.cb.set(resultid["id"], val)
            '''
             Critical section
            '''
            self.dateiter[tmpdate] -= 1
            if self.dateiter[tmpdate] == 0:
                self.dateiter.pop(tmpdate, None)
                self.cycledates = itertools.cycle(self.dateiter.keys())

            self.lock.release()
            print(self.dateiter)

    def run(self):
        import concurrent.futures
        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
            executor.submit(self.createdateset())


'''
    A = Docgen('/data/wikidata', '172.23.123.38', 'bucket-1')
    A.start_load()
    A = numeric('/data/wikidata', '172.23.123.38', 'bucket-1')
    A.start_load()
    A.create_document()
'''

A = Datefacet()
A.run()
