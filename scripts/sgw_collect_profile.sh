#!/usr/bin/env python

## Collect profile data as PDFs and create a .tar.gz file

import os, glob, re, shutil, sys, re, subprocess


profile_types = [
    "profile",
    "heap",
    "goroutine",
]

# keep track of all the profile pdfs that are generated so we can clean up later
profile_pdfs = []

sg_pprof_url = "http://localhost:4985/_debug/pprof"

def run_command(command):
    return_val = os.system(command)
    if return_val != 0:
        raise Exception("{0} failed".format(command))

def collect_profiles(tmp_dir, sg_binary_path):
    for profile_type in profile_types:
        print "Collecting profile for {0}".format(profile_type)
        pdf_filename = "{0}.pdf".format(profile_type)
        dest_path = os.path.join(tmp_dir, pdf_filename)
        profile_pdfs.append(dest_path)
        cmd = "go tool pprof -pdf {0} {1}/{2} > {3}".format(
            sg_binary_path,
            sg_pprof_url,
            profile_type,
            dest_path,
        )
        print cmd
        run_command(cmd)


def create_tar_gz(tmp_dir):
    dest_file = os.path.join(tmp_dir, "profile_data.tar.gz")
    run_command("tar cvfz {0} {1}/*.pdf".format(dest_file, tmp_dir))

def cleanup_files(tmp_dir):
    for profile_pdf in profile_pdfs:
        os.remove(profile_pdf)

if __name__ == "__main__":

    # get commit from command line args and validate
    if len(sys.argv) <= 2:
        raise Exception("Usage: {0} <path-sync-gw-binary> <tmp-dir>".format(sys.argv[0]))

    sg_binary_path = sys.argv[1]
    tmp_dir = sys.argv[2]

    collect_profiles(tmp_dir, sg_binary_path)

    create_tar_gz(tmp_dir)

    cleanup_files(tmp_dir)
