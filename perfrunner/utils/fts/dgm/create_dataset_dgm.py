import random

from couchbase.bucket import Bucket


class NumericExctractor:
    def __init__(self, cb_url, bucket_name, items):
        self.distance = 39000
        self.output_lines_goal = 1000
        self.total_docs = items
        self.cb = Bucket("couchbase://{}/{}?operation_timeout=10".format(cb_url, bucket_name), password="password")
        self.output_list = list()

    def run(self):
        n = 0
        i = 0
        while i<self.output_lines_goal:
            n, min = self.read_number(n)
            max = min + random.randint(999999, 9999999999)
            self.output_list.append("{}   max".format(max))
            self.output_list.append("{}   min".format(min))
            self.output_list.append("{}:{}   max_min".format(max, min))
            i += 1
            print(i)
        self.write_and_exit()

    def read_number(self, n):
        step = random.randint(0, self.distance)
        n = n + step
        if n >= self.total_docs:
            self.write_and_exit()
        k = hex(n)[2:]
        v = self.cb.get(k).value
        return n, int(v['time'])

    def write_and_exit(self):
        f = open("numeric-dgm.txt", "w")
        f.write('\n'.join(self.output_list))
        f.close()


class TermProcessor:
    def __init__(self):
        self.limit = 2000

    def _shuffle_and_cut(self, fpath, limit=500):
        f = open(fpath)
        lines = list(f)
        random.shuffle(lines)
        lines = lines[:limit]
        f.close()
        return lines

    def combine_2(self, output_name, file1, file2):
        lines1 = self._shuffle_and_cut(file1)
        lines2 = self._shuffle_and_cut(file2)

        map = set()
        for line1 in lines1:
            line1 = line1.split()[0]
            for line2 in lines2:
                line2 = line2.split()[0]
                if line1 != line2:
                    direct = "{} {}".format(line1, line2)
                    reverse = "{} {}".format(line2, line1)
                    if reverse not in map:
                        map.add(direct)
        results = list(map)
        random.shuffle(results)
        results = results[:self.limit]

        result_file = open(output_name, "w")
        for line in results:
            result_file.write("{}\n".format(line))
        result_file.close()

    def combine_3(self, output_name, file1, file2, file3):
        lines1 = self._shuffle_and_cut(file1)
        lines2 = self._shuffle_and_cut(file2)
        lines3 = self._shuffle_and_cut(file3)
        map = set()
        for line1 in lines1:
            line1 = line1.split()[0]
            for line2 in lines2:
                line2 = line2.split()[0]
                for line3 in lines3:
                    line3 = line3.split()[0]
                    if line2 != line3:
                        direct = "{} {} {}".format(line1, line2, line3)
                        reverse = "{} {} {}".format(line1, line3, line2)
                        if reverse not in map:
                            map.add(direct)
        results = list(map)
        random.shuffle(results)
        results = results[:self.limit]
        result_file = open(output_name, "w")
        for line in results:
            result_file.write("{}\n".format(line))
        result_file.close()

    def get_fuzzies(self, output1_file, output2_file, input_file, size=5):
        lines = self._shuffle_and_cut(input_file, 10 ** 6)
        sized_lines = list()
        items = 0
        for line in lines:
            line = line.split()[0]
            if len(line) == size:
                sized_lines.append(line)
                items = len(sized_lines)
            if items == self.limit:
                break

        output1_file = open(output1_file, "w")
        output2_file = open(output2_file, "w")
        for line in sized_lines:
            output1_file.write("{} {}\n".format(line, "1"))
            output2_file.write("{} {}\n".format(line, "2"))
        output1_file.close()
        output2_file.close()

    def get_wildcard(self, output_file, input_file):
        lines = self._shuffle_and_cut(input_file, self.limit)
        of = open(output_file, "w")
        for line in lines:
            line = line.split()[0]
            if len(line) == 4:
                term = line
                term = line[:2] + '*' + line[3:]
                of.write("{}\n".format(term))
        of.close()

    def get_prefix(self, output_file, input_file):
        lines = self._shuffle_and_cut(input_file, 10 ** 6)
        map = set()
        for line in lines:
            line = line.split()[0]
            if len(line) > 3:
                map.add(line[:3])
        map = list(map)[:self.limit]
        output_file = open(output_file, "w")
        for line in map:
            output_file.write("{}\n".format(line))
        output_file.close()

    def get_phrases(self, cb_url, output_file, input_file, docs_total):
        cb = Bucket("couchbase://{}/{}?operation_timeout=10".format(cb_url, "bucket-1"), password="password")
        lines = self._shuffle_and_cut(input_file, 10 ** 6)
        formatted_lines = list()
        for line in lines:
            formatted_lines.append(line.split()[0])
        lines = formatted_lines
        results = set()
        for docid in range(1, docs_total - 1):
            key = hex(random.randint(1, docs_total))[2:]
            try:
                txt = cb.get(key).value
                if txt["text"]:
                    txt = txt["text"].encode('ascii', 'ignore')
                    terms = txt.split(' ')
                    for idx, term in enumerate(terms):
                        if term in lines:
                            if len(terms) > idx + 1:
                                term_next = terms[idx + 1]
                                if str.isalpha(term_next):
                                    result_phrase = "{} {}".format(term, term_next)
                                    results.add(result_phrase)
            except Exception as e:
                print(("{}: {}: {}".format(key, len(results), str(e))))

            if len(results) > self.limit:
                break

        output_file = open(output_file, "w")
        for phrase in results:
            output_file.write("{}\n".format(phrase))

    def get_dates(self, output_file, input_file):
        lines = self._shuffle_and_cut(input_file, self.limit)
        output_file = open(output_file, "w")
        for line in lines:
            line = line.split()[0]

            y = random.randint(2007, 2017)
            m1 = random.randint(1, 12)
            m2 = random.randint(1, 12)
            if m1 > m2:
                b = m1
                m1 = m2
                m2 = b
            d1 = random.randint(1, 30)
            d2 = random.randint(1, 30)
            date1 = "{}-{}-{}".format(y, m1, d1)
            date2 = "{}-{}-{}".format(y, m2, d2)

            result = "{} {}:{}".format(line, date1, date2)
            output_file.write("{}\n".format(result))

'''
nex = NumericExctractor("172.23.99.211", "bucket-1", 20000000)
nex.run()

'''
termpro = TermProcessor()
#termpro.combine_2("hi_hi.txt", "hi.txt", "hi.txt")
#termpro.combine_2("hi_med.txt", "hi.txt", "med.txt")
#termpro.combine_3("hi_med_med.txt", "hi.txt", "med.txt", "med.txt")
#termpro.combine_3("med_hi_hi.txt", "med.txt", "hi.txt", "hi.txt")
termpro.get_fuzzies("fuzzy_1.txt", "fuzzy_2.txt", "med.txt")
termpro.get_wildcard("wildcard.txt", "med.txt")
#termpro.get_phrases("172.23.99.211", "phrase.txt", "med.txt", 1000000)
#termpro.get_dates("date.txt", "hi.txt")
