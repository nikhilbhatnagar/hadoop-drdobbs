#!/bin/bash
#
# Script to download Google Book Ngrams dataset and load into HDFS.
#

for i in $(seq 0 9); do
  unzip -cq googlebooks-eng-all-1gram-20090715-$i.csv.zip | hadoop fs -put - googleBooks/googlebooks-eng-all-1gram-20090715-$i.tsv
done
