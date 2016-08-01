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
                print (self.counts, file)
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
                print value, ' max'
            elif tmpcount % 3 == 1:
                print value, ' min'
            elif tmpcount % 3 == 2:
                upper = value << 1
                lower = value >> 1
                print upper, ':', lower, ' max_min'
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

'''
    A = Docgen('/data/wikidata', '172.23.123.38', 'bucket-1')
    A.start_load()
    A = numeric('/data/wikidata', '172.23.123.38', 'bucket-1')
    A.start_load()
    A.create_document()
'''
