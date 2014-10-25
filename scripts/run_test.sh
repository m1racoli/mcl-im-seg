#!/usr/bin/env bash
mvn package -DskipTests=true
mr-mcl io.mat.MatFileLoader -i /mnt/hgfs/aufnahmen/16112012_1.mat -o data -te 2 -hdfs -s 50 -t 60
mr-mcl mapred.SequenceInputJob -Dsigma.I=0.01 -Dsigma.X=0.25 -i data -o matrix -te 2 -r 2 -nsub 64 -cm -co -zk
#mr-mcl mcl -i matrix -o clustering -cm -co -zk -n -S 100
mr-mcl util.AbcOutput -i matrix -o /mnt/hgfs/mcl-im-seg/results/test/matrix.abc
mcl /mnt/hgfs/mcl-im-seg/results/test/matrix.abc --abc -S 100 -R 100 -te 2 -o /mnt/hgfs/mcl-im-seg/results/test/clustering
