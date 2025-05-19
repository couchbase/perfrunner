#!/bin/bash +x
# Upload files matching the pattern to S3 bucket
upload_files_to_s3() {
    local pattern=$1

    for FILE in $pattern; do
        [ -f "$FILE" ] || continue
        env/bin/aws s3 cp --quiet ${FILE} s3://perf-artifacts/${BUILD_TAG}/${FILE}
        echo "https://s3-us-west-2.amazonaws.com/perf-artifacts/${BUILD_TAG}/${FILE}"
    done
}

# Upload zip and prof files
# cbcollect zip files, profile files, and CAO collected logs
upload_files_to_s3 "*.zip *.prof cbopinfo-*.tar.gz"
