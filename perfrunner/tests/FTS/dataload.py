"""
This file is needed only for future reference to
load data from wiki data.
DOwnload link : http://people.apache.org/~mikemccand/enwiki-20120502-lines-1k.txt.lzma
"""
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

    @staticmethod
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


A = Docgen('/data/wikidata', '172.23.123.38', 'bucket-1')
A.start_load()
