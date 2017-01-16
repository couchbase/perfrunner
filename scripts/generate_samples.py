import os
import shutil
import time
import zipfile
from os import listdir

"""
This tool allows to generate samples zip file with certain number of files.
PLEASE NOTE: create sample with 60M files inside requires ~10 hours and 250G disk space.
"""

NUM_FILES = 60000000

pth = 'beer-sample/docs'


def zipdir(path):
    zipf = zipfile.ZipFile('beer-sample.zip', 'w', allowZip64=True)
    for root, dirs, files in os.walk(path):
        for file in files:
            zipf.write(os.path.join(root, file))
    zipf.close()


def prepare_root_folder(sample_path='/opt/couchbase/samples/beer-sample.zip'):
    shutil.copy(sample_path, '.')
    zip_ref = zipfile.ZipFile('beer-sample.zip', 'r')
    zip_ref.extractall()
    zip_ref.close()
    os.remove('beer-sample.zip')
    shutil.rmtree('beer-sample/design_docs')


def calc_needed_files():
    files = listdir(pth)
    print len(files), "files in folder", pth
    num_folders = int(NUM_FILES / (len(files) * 10))
    print "additionally will generate", num_folders, "folders by", \
        len(files) * 10, "files"
    return files, num_folders


def generate_files():
    start = time.time()
    for i in xrange(num_folders):
        if not os.path.exists('beer-sample' + str(i)):
            print "folder ", 'beer-sample' + str(i), "created"
            os.makedirs('beer-sample' + str(i))
        for j in xrange(10):
            for f in files:
                shutil.copy(pth + "/" + f,
                            'beer-sample' + str(i) + "/" + f.replace(
                                '.json', str(i * 10 + 1 + j) + '.json'))
            print len(files) * (j + 1 + i * 10), "files generated"

        print 'copy beer-sample' + str(i), pth
        shutil.copytree('beer-sample' + str(i), pth + '/beer-sample' + str(i))
        shutil.rmtree('beer-sample' + str(i))

    end = time.time()
    print "time to generate %s: %s" % (NUM_FILES, end - start)


if __name__ == '__main__':
    prepare_root_folder()
    files, num_folders = calc_needed_files()
    generate_files()
    zipdir('beer-sample')
    shutil.rmtree('beer-sample')
