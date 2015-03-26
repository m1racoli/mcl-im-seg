#!/bin/bash

set -e

exp_path=$1
shift
src_path=$1
shift
exp=$1
shift
src=$1
shift

cluster_file="$exp_path/$exp/clustering/part-r-00000"
	
if [ ! -f "$cluster_file" ]
then
	echo "cluster file $cluster_file not found"
	exit 1
fi

if [ $src -eq 1 ]
then
	# png file
	src_file="$src_path/$src/src/file.png"
elif [ $src -ge 5 ]
then
	# mat file
	src_file="$src_path/$src/src/file.mat"
else
	# jpg file
	src_file="$src_path/$src/src/file.jpg"
fi

if [ ! -f "$src_file" ]
then
	echo "src file $src_file not found"
	exit 1
fi

out_dir="$exp_path/$exp/result"

mr-mcl result -i $src_file -c $cluster_file -o $out_dir -hdfs $@