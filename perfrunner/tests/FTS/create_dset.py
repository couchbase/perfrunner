import glob
import itertools
import os
import re
from random import shuffle

from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
from nltk.tokenize import TweetTokenizer


'''
  Change the files here and run
  The file1 is randomized  as size
  is less
'''


class CreateData(object):
        @staticmethod
        def createandor(file1, file2):
            tfile = open(file1, 'r')

            tfile1 = open(file2, 'r')

            l = list(tfile1)
            shuffle(l)
            b = itertools.cycle(l)
            count = 0
            for c in tfile:
                term, freq = c.split()
                term1, freq = b.next().split()
                print term1, term, term
                count += 1
            tfile.close()

            tfile = open(file1, 'r')
            for c in tfile:
                term, freq = c.split()
                term1, freq = b.next().split()
                print term1, term, term

            tfile.close()

            tfile = open(file1, 'r')
            for c in tfile:
                term, freq = c.split()
                term1, freq = b.next().split()
                print term1, term, term

            tfile.close()
            tfile1.close()

        @staticmethod
        def createprefix(file1):
            file = open(file1, 'r')
            s = set()
            for c in file:
                term, freq = c.split()
                if len(term) > 3:
                    s.add(term[:3])
            file.close()
            for v in s:
                print v

        @staticmethod
        def perm(st):
            '''

            All regex are expected to be 4 character
            with one wildcard
            '''
            var = [None for x in range(4)]
            var[2] = '*'
            var[:2] = list(st[:2])
            var[3] = st[-1:]
            return ''.join(var)

        @staticmethod
        def createwildcard(file1):
            file1 = open(file1, 'r')
            s = set()
            count9 = 0
            for line in file1:
                term, freq = line.split()
                if len(term) > 4 and count9 % 9 == 0:
                    s.add(CreateData.perm(term))
                count9 += 1
            file1.close()
            for v in s:
                print v

        @staticmethod
        def createfuzzy(files, fuzzy):
            file1 = open(files, 'r')
            count9 = 0
            for line in file1:
                term, freq = line.split()
                if len(term) == 5 and count9 % 9 == 0:
                    print term, fuzzy
                count9 += 1
            file1.close()

        @staticmethod
        def findnextword(self, source, target):
            '''
             This part of code useful to find next matching character
             of a word in string for phrase
            '''
            for i, w in enumerate(source):
                if w == target:
                    if i < len(source) - 1:
                        return source[i + 1]

        def insert_cb(self, f):
            '''
            create phrase dataset
            '''
            f1 = open('midterm.txt', 'r')
            mids = []
            myset = set()
            for v in f1:
                s, _ = v.split()
                mids.append(s)
            for piece in self.read_in_chunks(f):
                test = {}
                piece = piece.replace('\n', ' ')
                test['text'] = piece
                for v in mids:
                    tknzr = TweetTokenizer()
                    sl = tknzr.tokenize(piece)
                    k = CreateData.findnextword(v, sl)
                    if k and k in mids:
                        sort = tuple(sorted((k, v)))
                        if sort not in self.myset:
                            print v, k
                            myset.add(sort)

        def read_in_chunks(self, file_object):
            """Lazy function (generator) to read a file piece by piece.
            Default chunk size: 1k."""
            while True:
                data = file_object.read(self.chunk_size)
                if not data:
                    break
                yield data

        @staticmethod
        def createnumeric(source):
            '''
              to create queries  for max , min , max_min
            '''
            c = 0
            file1 = open(source, 'r')
            for line in file1:
                term, freq = line.split()
                if term.isdigit():
                    if c % 3 == 0:
                        print term, 'max_min'
                    elif c % 3 == 1:
                        print term, 'max'
                    else:
                        print term, 'min'
                    c += 1

        @staticmethod
        def createids():
            cb = Bucket('couchbase://172.23.123.38/bucket-1', password='password')
            row_iter = cb.n1ql_query(N1QLQuery('select meta().id from `bucket-1` limit 10000'))
            for resultid in row_iter:
                '''
                use following to create the docids set
                '''
                print resultid["id"], None

        @staticmethod
        def changefiles():
            '''
             replacing all test files with Q with ''
             Not relevant to it, just storing.

             '''
            os.chdir('~/PERFTS/perfrunner/tests/fts')
            for file in glob.glob("*.test"):
                m = re.search('Q\d+_', file)
                newfile = file
                if m:
                    sub = m.group(0)
                    newfile = file.replace(sub, '')
                cmd = 'cp ' + file + ' news/' + newfile
                os.system(cmd)

        @staticmethod
        def generatebinary():
            '''
             This is used to generate numbers used for orderby
             parameters for .test file
             all strings generated will be always sorted in order
             '''
            for i in range(18):
                print i, 'q' + format(i, '#010b')

        @staticmethod
        def generate_test_fts():
            '''
            Here we will generate the fts part,
            '''
            pass


'''
    #AndHighOrMedMed
    #AndMedOrHighHigh
    CreateData.createandor('hiterm.txt', 'midterm.txt')
    CreateData.createprefix('midterm.txt')
    CreateData.createwildcard('midterm.txt')
    CreateData.createnumeric('midterm.txt')
    CreateData.createids()
    CreateData.changefiles()
    CreateData.createfuzzy('midterm.txt', 1)
'''
