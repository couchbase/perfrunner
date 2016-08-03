import glob
import itertools
import os
import re
from random import shuffle

from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery


'''
  Change the files here and run
  The file1 is randomized  as size
  is less
'''


class CreateData(object):
        @staticmethod
        def createandor(file1, file2):
            file = open(file1, 'r')
            file1 = open(file2, 'r')

            l = list(file1)
            shuffle(l)
            b = itertools.cycle(l)

            for c in file:
                term, freq = c.split()
                term1, freq = b.next().split()
                print term, term1

            file.close()
            file1.close()

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
            for line in file1:
                term, freq = line.split()
                if len(term) > 4:
                    s.add(CreateData.perm(term))
            file1.close()
            for v in s:
                print v

        @staticmethod
        def createfuzzy(files, fuzzy):
            file1 = open(files, 'r')
            for line in file1:
                term, freq = line.split()
                if len(term) == 5:
                    print term, fuzzy
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
    CreateData.createandor('midterm.txt', 'hiterm.txt')
    CreateData.createprefix('midterm.txt')
    CreateData.createwildcard('midterm.txt')
    CreateData.createnumeric('midterm.txt')
    CreateData.createids()
    CreateData.changefiles()
'''
