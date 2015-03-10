#!/usr/bin/env python

# Archives current seriesly databases in a rolling fashion
#
# Before 1st round of archiving
#
# - gateload_1.couch
# - gateload_2.couch
# - gateway_1.couch
# - gateway_2.couch
#
# After 1st round of archiving
#
# - gateload_1_archive_1.couch (previously *_1.couch)
# - gateload_2_archive_1.couch (previously *_1.couch)
# - gateway_1_archive_1.couch (previously *_1.couch)
# - gateway_2_archive_1.couch (previously *_1.couch)
#
# Before 2nd round of archiving
#
# - gateload_1.couch
# - gateload_2.couch
# - gateway_1.couch
# - gateway_2.couch
# - gateload_1_archive_1.couch
# - gateload_2_archive_1.couch
# - gateway_1_archive_1.couch
# - gateway_2_archive_1.couch
#
# After 2nd round of archiving
#
# - gateload_1_archive_1.couch (previously *_1.couch)
# - gateload_2_archive_1.couch (previously *_1.couch)
# - gateway_1_archive_1.couch (previously *_1.couch)
# - gateway_2_archive_1.couch (previously *_1.couch)
# - gateload_1_archive_2.couch (previously *_archive_1.couch)
# - gateload_2_archive_2.couch (previously *_archive_1.couch)
# - gateway_1_archive_2.couch (previously *_archive_1.couch)
# - gateway_2_archive_2.couch (previously *_archive_1.couch)
#

import os, glob, re, shutil

NUM_ARCHIVES = 100
SERIESLY_ROOT = "/root/seriesly-data"

def shift_existing_archives():
    '''
    shift all existing archives up one (archive_1 -> archive_2, etc)
    '''

    # find the latest archive (eg, archive_2)
    latest_archive_num = find_latest_archive_number()

    print "latest_archive_num: {0}".format(latest_archive_num)

    if latest_archive_num is None:
        print "nothing to do, no latest_archive_num found"
        return

    # walk the list of archives backward, for each one:
    for i in xrange(latest_archive_num, 0, -1):
        # shift it up (eg, archive_2 -> archive_3)
        shift_archive_up(i)


def shift_archive_up(archive_number):
    '''
    shift it up (eg, *archive_2 -> *archive_3)
    '''

    print "shift archive up: {0}".format(archive_number)

    # find all files that contain "archive_2" in the name
    files = archive_files(archive_number)

    # for each one, rename it to be "archive_3"
    for file_to_shift in files:
        shift_file_up(archive_number, file_to_shift)

def shift_file_up(archive_number, file_to_shift):
    '''
    gateload_2_archive_2.couch -> gateload_2_archive_3.couch
    '''
    shifted_filename = shifted_up_filename(archive_number, file_to_shift)
    src_path = os.path.join(SERIESLY_ROOT, file_to_shift)
    dest_path = os.path.join(SERIESLY_ROOT, shifted_filename)
    print "move {0} -> {1}".format(src_path, dest_path)
    shutil.move(src_path, dest_path)

def shifted_up_filename(archive_number, file_to_shift):
    '''
    gateload_2_archive_2.couch -> gateload_2_archive_3.couch
    '''
    shifted_archive_number = archive_number + 1
    search_pattern = "archive_{0}".format(archive_number)
    replace_pattern = "archive_{0}".format(shifted_archive_number)
    shifted_filename = re.sub(search_pattern, replace_pattern, file_to_shift)
    return shifted_filename

def find_latest_archive_number():
    '''
    find the latest archive (eg, archive_2)
    '''

    # loop until an existing file is found
    highest_possible_archive_num = NUM_ARCHIVES * 2
    for i in xrange(highest_possible_archive_num, 0, -1):
        print "Checking if {0} exists".format(i)
        if archive_number_exists(i):
            return i

    return 0


def garbage_collect_dead_archives():
    '''
    delete any archives who have an archive number > NUM_ARCHIVES
    '''
    print "Garbage collecting dead archives"
    for i in xrange(10000): # make it a finite loop in case of bugs

        latest_archive_num = find_latest_archive_number()
        print "Latest archive number: {0}".format(latest_archive_num)
        if latest_archive_num <= NUM_ARCHIVES:
            return

        delete_archive(latest_archive_num)

def archive_files(archive_number):
    '''
    find all files that contain "archive_{archive_number}" in the name
    '''
    print "archive_files called with {0}.  seriestly_root: {1}".format(archive_number, SERIESLY_ROOT)
    files = glob.glob("{0}/*archive_{1}*".format(SERIESLY_ROOT, archive_number))
    files_path_stripped = []
    for archive_file in files:
        files_path_stripped.append(os.path.basename(archive_file))
    return files_path_stripped

def delete_archive(archive_number):

    print "Deleting archive number: {0}".format(archive_number)

    files = archive_files(archive_number)

    # for each one, delete it
    for deletable_file in files:
        os.remove(os.path.join(SERIESLY_ROOT,deletable_file))


def archive_number_exists(archive_number):

    print "archive_number_exists called with {0}".format(archive_number)

    # do any files contain "archive_2" in their name?
    files = archive_files(archive_number)
    return len(files) > 0


def find_non_archive_files():
    files = glob.glob("{0}/*".format(SERIESLY_ROOT))
    non_archive_files = []
    for cur_file in files:
        cur_file_basename = os.path.basename(cur_file)
        if "archive" not in cur_file_basename:
            non_archive_files.append(cur_file_basename)
    return non_archive_files

def archive_current_results():
    '''
    find all files that don't contain "archive" in name
    and move each one to {filename}_archive_1.
    gateload_1.couch -> gateload_1_archive_1.couch
    '''
    non_archive_files = find_non_archive_files()
    for non_archive_file in non_archive_files:
        # gateload_1.couch -> "gateload_1", "couch"
        filename = os.path.splitext(non_archive_file)[0]
        extension = os.path.splitext(non_archive_file)[1]
        # "gateload_1" -> "gateload_1_archive_1"
        new_filename = "{0}_archive_1{1}".format(filename, extension)
        src_path = os.path.join(SERIESLY_ROOT, non_archive_file)
        dest_path = os.path.join(SERIESLY_ROOT, new_filename)
        print "move {0} -> {1}".format(src_path, dest_path)
        shutil.move(src_path, dest_path)


if __name__ == "__main__":

    shift_existing_archives()

    garbage_collect_dead_archives()

    archive_current_results()
