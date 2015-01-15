#!/usr/bin/env bash
mvn package -DskipTests=true
mr-mcl io.mat.MatFileLoader -i /mnt/hgfs/aufnahmen/16112012_1.mat -o data -te 2 -hdfs -s 50 -t 55
mr-mcl mapred.SequenceInputJob -Dsigma.I=0.01 -Dsigma.X=2.0 -i data -o matrix -te 2 -r 4 -nsub 64 -cm -co -zk -fo 0
#mr-mcl mcl -i matrix -o clustering -cm -co -zk -n -S 100
mr-mcl rmcl -i matrix -o clustering -cm -co -zk -n -S 100
#mr-mcl util.AbcOutput -i matrix -o /mnt/hgfs/mcl-im-seg/results/test/matrix.abc
#mcl /mnt/hgfs/mcl-im-seg/results/test/matrix.abc --abc -S 100 -R 100 -te 2 -I 1.2 -o /mnt/hgfs/mcl-im-seg/results/test/clustering
