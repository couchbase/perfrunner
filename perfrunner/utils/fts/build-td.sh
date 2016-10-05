#!/bin/sh

#
The location of the script in question is:

https://github.com/couchbase/cbft/blob/master/cmd/cbft-build-td


1.  This is done with a new script we wrote, it can be found here:

FIXME

This script is not shipped as a part of the binary distribution, so you need to acquire it some other way.

2.  The script will invoke other cbft command-line tools which ARE a part of the binary distribution.  This means that you couchbase binaries must be on the path.  For me to fix that here I run:

$ export PATH=$PATH:/Users/mschoch/Documents/research/cbsource/install/bin

This will be /opt/couchbase/bin for most users.

3.  Run the script:

$ ./cbft-build-td perf_fts_index text 1000000

Argument 1 is a pattern matching the index name.  In this case i used "perf_fts_index", internally we match this against $1_*.pindex.

Argument 2 is the name of the field you want to analyze.  The wikipedia documents have 1 field named "text".

Argument 3 is the total number of documents, this is used to calculate the hi/med/low cut-offs.

The script will create a secure temp file, something like : /tmp/td-perf_fts_index.I8NcX5
The script will open each pindex file (this generates some FDB info message).
It will print the name of the file it is currently working on.
The script will keep appending data to the temp file.
After processing all the files it will sort the data and print a line like:
sorting into /tmp/td-perf_fts_index.I8NcX5.sorted
This has the same prefix as the original temp file, with the ".sorted" extension.
Next, after sorting it must combine all consecutive rows for the same term.
It will print a message line:
merging into /tmp/td-perf_fts_index.I8NcX5.merged
Again, same temp file prefix with new suffix ".merged".

Finally it will make several passes over this ".merged" file to find data falling into the hi/med/low buckets.  You will see output like:

getting high (90000.00 <= x < 900000.0) - /tmp/td-perf_fts_index.I8NcX5.hi
getting med (2000.000 <= x < 90000.00) - /tmp/td-perf_fts_index.I8NcX5.med
getting low (500.0000 <= x < 2000.000) - /tmp/td-perf_fts_index.I8NcX5.low

These files contain the the hi/med/low terms and their counts (space separated)

Finally, as a sanity check we print the counts of terms that were either too high or too low:

too high: 2
too low: 2963229

Here is the output from a sample run:

https://gist.github.com/mschoch/daeb32b6d8fd5c6255cfb8b143b6d238

If you have questions let me know.
#
# set this is couchbase bin is not in your path
COUCHBASE_PATH=""

HIGH_HIGH=0.9
HIGH_MED=0.09
LOW_MED=0.002
LOW_LOW=0.0005

tmpfile=$(mktemp /tmp/td-$1.XXXXXX)
echo "tmpfile for collation is $tmpfile"

for f in $1_*.pindex
do
  echo "Processing $f file..."
  ${COUCHBASE_PATH}cbft-bleve-dump -dictionary $2 -index $f | awk '{print $1,$3}' >> $tmpfile
done

# sort by col1
echo "sorting into $tmpfile.sorted"
sort -t ' ' -k1,1 $tmpfile > ${tmpfile}.sorted

# merge rows with same first column, sum second column
echo "merging into $tmpfile.merged"
awk -F ' ' '{
  if($1==k) {
    sum+=$2
  } else if(NR!=1){
    printf("%s %d\n",k,sum)
    sum=$2
  }
  k=$1
}' ${tmpfile}.sorted > ${tmpfile}.merged

HH=$(bc <<<"scale=0;$HIGH_HIGH*$3")
HM=$(bc <<<"scale=0;$HIGH_MED*$3")
LM=$(bc <<<"scale=0;$LOW_MED*$3")
LL=$(bc <<<"scale=0;$LOW_LOW*$3")

# find hi,med,low terms
echo "getting high ($HM <= x < $HH) - ${tmpfile}.hi"
awk -v hh="$HH" -v hm="$HM" -F ' ' '{
  if(($2 < hh) && ($2 >= hm)) {
    printf("%s %d\n",$1,$2)
  }
}' ${tmpfile}.merged > ${tmpfile}.hi

echo "getting med ($LM <= x < $HM) - ${tmpfile}.med"
awk -v hm="$HM" -v lm="$LM" -F ' ' '{
  if(($2 < hm) && ($2 >= lm)) {
    printf("%s %d\n",$1,$2)
  }
}' ${tmpfile}.merged > ${tmpfile}.med

echo "getting low ($LL <= x < $LM) - ${tmpfile}.low"
awk -v lm="$LM" -v ll="$LL" -F ' ' '{
  if(($2 < lm) && ($2 >= ll)) {
    printf("%s %d\n",$1,$2)
  }
}' ${tmpfile}.merged > ${tmpfile}.low

# check too high
awk -v hh="$HH" -F ' ' 'BEGIN{
  count = 0
}{
  if($2 >= hh) {
    count++
  }
}
END{
  printf("too high: %d\n", count)
}' ${tmpfile}.merged

# check too low
awk -v ll="$LL" -F ' ' 'BEGIN{
  count = 0
}{
  if($2 < ll) {
    count++
  }
}
END{
  printf("too low: %d\n", count)
}' ${tmpfile}.merged