
import datetime
import itertools

import numpy
from couchbase.bucket import Bucket


class GenericDocgen:
    def __init__(self, master_file_path, cb_url, bucket_name, chunk_size=1024):
        self.chunk_size = chunk_size
        self.items = 100
        self.master_file_path = master_file_path
        self.dateBuilder = DateBuilder()
        self.numBuilder = NumericBuilder(self.items)

        self.c = Bucket("couchbase://{}/{}?operation_timeout=10".format(cb_url, bucket_name),
                        password="password")

    def read_file_gen(self, file):
        while True:
            data = file.read(self.chunk_size)
            if not data:
                break
            yield data

    def build(self, master_file):
        self.master_lines = self.read_file_gen(master_file)
        doc = {}
        doc["text"] = self.master_lines.next()
        doc["text2"] = self.master_lines.next()
        doc["time"] = self.numBuilder.build()
        doc["date"] = self.dateBuilder.build()
        return doc

    def run(self):
        master_file = open(self.master_file_path)
        i = 0
        while i < self.items:
            key = hex(i)[2:]
            val = self.build(master_file)
            self.c.upsert(key, val)
            try:
                saved = self.c.get(key).value
                if 'text' in saved:
                    i += 1
            except Exception:
                pass
        master_file.close()


class NumericBuilder:
    def __init__(self, items):
        self.items = items
        self.timenow = 0
        self.month = itertools.cycle(range(1, 12))
        self.days = itertools.cycle(range(1, 28))
        self.hours = itertools.cycle(range(24))
        self.minutes = itertools.cycle(range(60))
        self.seconds = itertools.cycle(range(60))
        self.milis = itertools.cycle(range(48000, 483163))
        self.years_list = list(range(2007, 2017))
        self.years_weighted = [self.years_list[9]] * int(0.3  * items) + \
                              [self.years_list[8]] * int(0.17 * items) + \
                              [self.years_list[7]] * int(0.12 * items) + \
                              [self.years_list[6]] * int(0.09 * items) + \
                              [self.years_list[5]] * int(0.08 * items) + \
                              [self.years_list[4]] * int(0.06 * items) + \
                              [self.years_list[3]] * int(0.05 * items) + \
                              [self.years_list[2]] * int(0.05 * items) + \
                              [self.years_list[1]] * int(0.04 * items) + \
                              [self.years_list[0]] * int(0.04 * items)
        self.years_weighted.reverse()
        self.years = itertools.cycle(self.years_weighted)

    def build(self):
        return self.time_milis()

    def form_date(self):
        return (
            next(self.years),
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



class DateBuilder:
    def __init__(self):
        yrange = range(2007, 2017)
        yrange.reverse()

        mrange = range(1, 12)
        mrange.reverse()

        drange = range(1, 30)
        drange.reverse()

        self.years = itertools.cycle(yrange)
        self.month = itertools.cycle(mrange)
        self.days = itertools.cycle(drange)
        self.dates_list = list()
        self.total_size = 10*12*30
        self.form_dates_list()

    def form_dates_list(self):
        for _ in range(self.total_size):
            self.dates_list.append('{}-{}-{}'.format(next(self.years),
                                                     next(self.month),
                                                     next(self.days)))

    def build(self):
        index = self.zipf()
        while index[0] >= len(self.dates_list):
            index = self.zipf()
        return self.dates_list[index[0]]

    def zipf(self):
        return numpy.random.zipf([1.2])


docgen = GenericDocgen('output.txt', '172.23.99.211', 'bucket-1')
docgen.run()

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
