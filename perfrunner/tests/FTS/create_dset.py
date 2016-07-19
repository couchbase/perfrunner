import itertools
from random import shuffle
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
        def createprefix(file1, file2):
            file = open(file1, 'r')
            file1 = open(file2, 'r')

            l = list(file1)
            shuffle(l)
            b = itertools.cycle(l)

            for c in file:
                term, freq = c.split()
                term1, freq = b.next().split()
                if len(term) > 3:
                    print term[:3], freq
                if len(term1) > 3:
                    print term1[:3], freq

            file.close()
            file1.close()

        @staticmethod
        def perm(st, freq):
            '''

            All regex are expected to be 4 character
            with one wildcard
            '''
            var = [0 for i in range(4)]
            for v in range(4):
                var[v] = '*'
                if v == 0:
                    var[1:] = list(st[-3:])
                elif v == 1:
                    var[0] = st[0]
                    var[2:] = list(st[-2:])
                elif v == 2:
                    var[:2] = list(st[:2])
                    var[3] = st[-1:]
                else:
                    var[:3] = list(st[:3])
                print ''.join(var), freq

        @staticmethod
        def createwildcard(file1):
            file1 = open(file1, 'r')
            for line in file1:
                term, freq = line.split()
                if len(term) > 4:
                    CreateData.perm(term, freq)
            file1.close()

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

'''
    CreateData.createandor('midterm.txt', 'hiterm.txt')
    CreateData.createprefix('midterm.txt', 'hiterm.txt')
    CreateData.createwildcard('midterm.txt')
    CreateData.createnumeric('midterm.txt')
'''
