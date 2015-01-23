#!/usr/bin/env bash

set -e
set -o xtrace

data=$1
shift
inf=$1
shift
bal=$1
shift
exp=$1
shift

target="s3n://mcl-tests/exp/$exp"
log="$target/log.txt"
export HADOOP_CLIENT_OPTS="-Xmx2G"

mr-mcl bmcl -a -b $bal --change-limit 5.0e-5 --local -n -I $inf -S 100 -i "$data/r3_128" -o "$target" -l "$log" -cm -co -vint -zk $@
mr-mcl util.StreamedNcut -i "$data/r3_abc/matrix.abc" -o some "$target/clustering"
