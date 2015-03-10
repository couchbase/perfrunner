#!/usr/bin/env python

# Installs sync gateway from source
# 
# This happens when the user to perfrunner (via Jenkins typically) enters a version like:
# 
#   commit:<commit-hash>
# 
# rather than the typical version string:
# 
#   x.y.z (eg, 0.0.1-343)
#
# It is needed in order to generate PDF output from the go profiler (eg, http://filebin.ca/1uGqk8MqwTrY/profile_cpu_3.pdf)


import os, glob, re, shutil, sys

SYNC_GW_SRC_DIR="/tmp/sync_gateway_src"
SYNC_GW_CLONE_DIR=os.path.join(SYNC_GW_SRC_DIR, "sync_gateway")

def run_command(command):
    return_val = os.system(command)
    if return_val != 0:
        raise Exception("{0} failed".format(command))

if __name__ == "__main__":

    # get commit from command line args and validate
    if len(sys.argv) <= 1:
        raise Exception("Usage: {0} <commit-hash>".format(sys.argv[0]))
    commit = sys.argv[1]
    if commit is None or len(commit) == 0:
        raise Exception("Must provide valid commit")



    # mkdir + chdir 
    if not os.path.exists(SYNC_GW_SRC_DIR):
        os.mkdir(SYNC_GW_SRC_DIR)
    os.chdir(SYNC_GW_SRC_DIR)

    # delete current contents
    if os.path.exists(SYNC_GW_CLONE_DIR):
        shutil.rmtree(SYNC_GW_CLONE_DIR)

    # clone repo 
    run_command("git clone https://github.com/couchbase/sync_gateway.git")

    # cd into clone dir
    os.chdir(SYNC_GW_CLONE_DIR)

    # checkout commit 
    print "Checking out commit: {0}".format(commit)
    run_command("git checkout {0}".format(commit))

    # checkout submodules
    run_command("git submodule init")
    run_command("git submodule update")

    # build and test
    run_command("./build.sh")
    run_command("./test.sh -cpu 4 -race")

    # create target dir if it doesn't exist
    if not os.path.exists("/opt/couchbase-sync-gateway/bin"):
        os.mkdir("/opt/couchbase-sync-gateway/bin")

    # copy binary to /opt/sync_gateway 
    shutil.copy("bin/sync_gateway","/opt/couchbase-sync-gateway/bin/sync_gateway")
