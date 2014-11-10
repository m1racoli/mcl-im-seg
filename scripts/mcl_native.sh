#!/usr/bin/env bash

set -e

if [ $# = 0 ]; then
	echo "need one argument for, ie. 'sample1'"
	exit 1
fi

sample=$1
shift
nbucket="s3n://mcl-tests/samples"
bucket="s3://mcl-tests/samples"
basedir="/tmp/samples"
sigmaX="4.0"
sigmaF="0.01"
radius="5"
te="4"
inf="2.0"
S="100"

mkdir -p "$basedir/$sample"
aws s3 sync "$bucket/$sample" "$basedir/$sample"

#create abc from image
mr-mcl util.ImageTool -sF "$sigmaF" -sX "$sigmaX" -r "$radius" i -cielab -i "$basedir/$sample/src" -o "$basedir/$sample/abc/matrix.abc" -te "$te" 
#make abc

mcl "$basedir/$sample/abc/matrix.abc" --abc -te "$te" -I "$inf" -S "$S" -odir "$basedir/$sample/clustering" -o "clustering"
#mvn package -DskipTests=true
#mr-mcl io.mat.MatFileLoader -i /mnt/hgfs/aufnahmen/16112012_1.mat -o data -te 2 -hdfs -s 50 -t 55
#mr-mcl mapred.SequenceInputJob -Dsigma.I=0.01 -Dsigma.X=2.0 -i data -o matrix -te 2 -r 4 -nsub 64 -cm -co -zk -fo 0
#mr-mcl mcl -i matrix -o clustering -cm -co -zk -n -S 100
#mr-mcl rmcl -i matrix -o clustering -cm -co -zk -n -S 100
#mr-mcl util.AbcOutput -i matrix -o /mnt/hgfs/mcl-im-seg/results/test/matrix.abc
#mcl /mnt/hgfs/mcl-im-seg/results/test/matrix.abc --abc -S 100 -R 100 -te 2 -I 1.2 -o /mnt/hgfs/mcl-im-seg/results/test/clustering
