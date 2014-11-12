#!/usr/bin/env bash

if [ $# -lt 4 ]; then
	echo "usage: script.sh <sample> <sigmaX> <sigmaF> <format>"
	exit 1
fi

set -e
set -o xtrace

sample=$1
shift
nbucket="s3n://mcl-tests/samples"
bucket="s3://mcl-tests/samples"
basedir="/tmp/samples"
sigmaX=$1
shift
sigmaF=$1
shift
format=$1
shift
radius="5"
te="4"
S="100"
R="100"

if [ "$format" = "jpg" ]; then
	class="util.ImageTool"
	cielab="-cielab"
elif [ "$format" = "mat" ]; then
	class="util.MatTool"
	cielab=""
else
	echo "invalid format $format"
	exit 1
fi

mkdir -p "$basedir/$sample/src"
aws s3 sync "$bucket/$sample/src" "$basedir/$sample/src"

#create abc from image
mkdir -p "$basedir/$sample/abc"
mr-mcl $class -sF "$sigmaF" -sX "$sigmaX" -r "$radius" -i "$basedir/$sample/src" -o "$basedir/$sample/abc/matrix.abc" -te "$te" $cielab "$@"
#make abc
mkdir -p "$basedir/$sample/clustering"
rm -f "$basedir/$sample/clustering/*"

for inf in "1.2" "1.4" "1.6" "1.8" "2.0"
do
	mcl "$basedir/$sample/abc/matrix.abc" --abc -te "$te" -I "$inf" -S "$S" -R "$R" -o "$basedir/$sample/clustering/mcl.$inf"
done

aws s3 sync "$basedir/$sample" "$bucket/$sample"

#mvn package -DskipTests=true
#mr-mcl io.mat.MatFileLoader -i /mnt/hgfs/aufnahmen/16112012_1.mat -o data -te 2 -hdfs -s 50 -t 55
#mr-mcl mapred.SequenceInputJob -Dsigma.I=0.01 -Dsigma.X=2.0 -i data -o matrix -te 2 -r 4 -nsub 64 -cm -co -zk -fo 0
#mr-mcl mcl -i matrix -o clustering -cm -co -zk -n -S 100
#mr-mcl rmcl -i matrix -o clustering -cm -co -zk -n -S 100
#mr-mcl util.AbcOutput -i matrix -o /mnt/hgfs/mcl-im-seg/results/test/matrix.abc
#mcl /mnt/hgfs/mcl-im-seg/results/test/matrix.abc --abc -S 100 -R 100 -te 2 -I 1.2 -o /mnt/hgfs/mcl-im-seg/results/test/clustering
