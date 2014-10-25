#!/usr/bin/env bash
mvn package -DskipTests=true
mr-mcl io.mat.MatFileLoader -i /mnt/hgfs/aufnahmen/16112012_1.mat -o data -te 2 -hdfs -s 50 -t 60
mr-mcl mapred.SequenceInputJob -Dsigma.I=0.01 -Dsigma.X=0.25 -i data -o matrix -te 2 -nsub 8 -cm -co -zk
mr-mcl mcl -i matrix -o clustering -cm -co -zk -n
