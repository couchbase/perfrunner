import random

from couchbase.bucket import Bucket


class TestDataset:
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
            print(line, file=result_file)
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
            print(line, file=result_file)
        result_file.close()

    def get_fuzzies(self, output1_file, output2_file, input_file, size=5):
        lines = self._shuffle_and_cut(input_file, 10**6)
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
            print("{} {}".format(line, "1"), file=output1_file)
            print("{} {}".format(line, "2"), file=output2_file)
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
                print(term, file=of)
        of.close()

    def get_prefix(self, output_file, input_file):
        lines = self._shuffle_and_cut(input_file, 10**6)
        map = set()
        for line in lines:
            line = line.split()[0]
            if len(line) > 3:
                map.add(line[:3])
        map = list(map)[:self.limit]
        output_file = open(output_file, "w")
        for line in map:
            print(line, file=output_file)
        output_file.close()

    def get_dates(self, output_file, input_file):
        lines = self._shuffle_and_cut(input_file, self.limit)
        dates = ['2013-10-17', '2013-11-17', '2014-02-09', '2015-11-26']
        output_file = open(output_file, "w")
        for line in lines:
            line = line.split()[0]
            result = "{} {} {}".format(line, dates[random.randint(0, 1)], dates[random.randint(2, 3)])
            print(result, file=output_file)

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
            print(phrase, file=output_file)


datagen = TestDataset()
datagen.combine_2("hi_hi.txt", "hi.txt", "hi.txt")
datagen.combine_2("hi_med.txt", "hi.txt", "med.txt")
datagen.combine_3("hi_med_med.txt", "hi.txt", "med.txt", "med.txt")
datagen.combine_3("med_hi_hi.txt", "med.txt", "hi.txt", "hi.txt")
datagen.get_fuzzies("fuzzy_1.txt", "fuzzy_2.txt", "med.txt")
datagen.get_wildcard("wildcard.txt", "med.txt")
datagen.get_wildcard("wildcard_low.txt", "low.txt")
datagen.get_prefix("prefix.txt", "med.txt")
datagen.get_phrases("172.23.99.211", "phrase.txt", "med.txt", 1000000)
datagen.get_dates("date.txt", "hi.txt")
