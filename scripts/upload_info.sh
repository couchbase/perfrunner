#!/bin/bash +x

for FILE in *.zip; do
    env/bin/aws s3 cp --quiet ${FILE} s3://perf-artifacts/${BUILD_TAG}/${FILE}
    echo "https://s3-us-west-2.amazonaws.com/perf-artifacts/${BUILD_TAG}/${FILE}"
done

for FILE in *.prof; do
    [ -f "$FILE" ] || continue
    env/bin/aws s3 cp --quiet ${FILE} s3://perf-artifacts/${BUILD_TAG}/${FILE}
    echo "https://s3-us-west-2.amazonaws.com/perf-artifacts/${BUILD_TAG}/${FILE}"
done
