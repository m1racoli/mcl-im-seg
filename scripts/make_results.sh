#!/bin/bash
# this script generates results

#set -e
set -x

exp_path=$1
shift
src_path=$1
shift

index_file="$exp_path/index.txt"

if [ ! -f "$index_file" ]
then
	echo "index file $index_file not found"
	exit 1
fi

while read p; do
	exp=$(echo $p | cut -f1 -d,)
	src=$(echo $p | cut -f2 -d,)
	
	
	scripts/make_result.sh $exp_path $src_path $exp $src
	
	#if [ ! $? -eq 0 ]
	#then
	#	continue
	#fi
	
done <$index_file

echo "finished"
exit 0
