#!/usr/bin/env python

## Collect profile data as PDFs and create a .tar.gz file

import os, shutil, sys, tempfile

profile_types = [
    "profile",
    "heap",
    "goroutine",
]

format_types = [
    "pdf",
    "text",
]

sg_pprof_url = "http://localhost:4985/_debug/pprof"

def run_command(command):
    return_val = os.system(command)
    if return_val != 0:
        raise Exception("{0} failed".format(command))

def collect_profiles(tmp_dir, sg_binary_path):

    # make sure raw profile gz files end up in tmp dir 
    os.environ["PPROF_TMPDIR"] = tmp_dir

    for profile_type in profile_types:
        for format_type in format_types:
            print "Collecting {0} profile in format {1}".format(profile_type, format_type)
            out_filename = "{0}.{1}".format(profile_type, format_type)
            dest_path = os.path.join(tmp_dir, out_filename)
            cmd = "go tool pprof -{0} {1} {2}/{3} > {4}".format(
                format_type,
                sg_binary_path,
                sg_pprof_url,
                profile_type,
                dest_path,
            )
            print cmd
            run_command(cmd)


def create_tar_gz(tmp_dir):
    dest_file = result_file(tmp_dir)
    cwd = os.getcwd()
    os.chdir(tmp_dir)
    run_command("tar cvfz {0} *".format(dest_file))
    os.chdir(cwd)

def result_file(tmp_dir):
    return os.path.join(tmp_dir, "profile_data.tar.gz")

def copy_result(tmp_dir, dest_dir):
    src_file = result_file(tmp_dir)
    dest_file = os.path.join(dest_dir, "profile_data.tar.gz")
    shutil.copyfile(src_file, dest_file)

if __name__ == "__main__":

    # get commit from command line args and validate
    if len(sys.argv) <= 2:
        raise Exception("Usage: {0} <path-sync-gw-binary> <dest-dir>".format(sys.argv[0]))

    sg_binary_path = sys.argv[1]

    # this is where the profile_data.tar.gz result will be stored
    dest_dir = sys.argv[2]

    # this is the temp dir where collected files will be stored.  will be deleted at end.
    tmp_dir = tempfile.mkdtemp()
    
    collect_profiles(tmp_dir, sg_binary_path)

    create_tar_gz(tmp_dir)

    copy_result(tmp_dir, dest_dir)

    # delete the tmp dir since we're done with it
    shutil.rmtree(tmp_dir) 
