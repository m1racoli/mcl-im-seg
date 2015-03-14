# Large Scale Image Segmentation Using Markov Flows #

This repository contains an implementation of the Markov Clustering (MCL) Algorithm and Regularized Markov Clustering (R-MCL) Algorithm using the Hadoop MapReduce framework and utilities to apply those algorithms to image segmentation.

### Requirements ###

* Linux (although not tested on Mac OS X)
* Java 7+, 64-bit
* Hadoop 2.4+
* Maven 3+ (for building)
* Cmake and gcc (for building native)
* Zookeeper 3.4.5+ (only of not using embedded Zookeeper server)

Furthermore in general do file and folder parameters denote files and folders in the Hadoop FileSystem (HDFS). Thus paths in Amazon S3 are supported using **s3://<bucket>/<key>** (or **s3n://<bucket>/<key>**) and local files need to be referred to using **file://<path>**.

## Building ##

The project is a standard Maven project. Running
```
#!Shell

mvn clean install
```
is sufficient to build the Java implementation. For being able to run the native implementation, the project has to be build with
```
#!Shell

mvn clean install -Pnative
```

## Running ##
The whole procedure of segmenting an image consists of four steps. Except for the first step, running HDFS recommended. For the second and third step a running ZooKeeper server is required (where the embedded server can be run via the -zk flag) for distributed metrics. If the embedded server is not used, then the Hadoop configuration should contain the options **mcl.zk.hosts** and **zk.metric.path**.

### Extract Pixel similarities ###
First the pixel similarity information has to be extracted from the image material. Possible image formats are Java supported image formats (i.e. jpg, png, ...). Running
```
#!Shell

mr-mcl load-img -i <input> -o <output> [-r <radius>] [-sF <sigmaF>] [-sX <sigmax>] [-cielab] [-te <threading>]
```
reads the image(s) and generates a matrix representation in ABC format with the following options:

* -i: the **local** input file/folder
* -o: the **local** output folder
* -r: the radius in which pixel similarities are considered (default: 3.0)
* -sF: scale factor for the value differences (default: 1.0)
* -sX: scale factor for the spatial differences (default: 1.0)
* -cielab: calculate pixel similarities with respect to the CIElab colour space
* -te: number of threads to use and number of output files (default: 1)

This step is done outside the Hadoop framework and thus doesn't require to have running HDFS or YARN.

#### ToF Images ####
Furthermore are time of flight images (ToF) in *.mat files supported, where the Z coordinates (representing the depth of the pixel within the spatial space of the image) are stored on a numeric array named "Z".
```
#!Shell

mr-mcl load-mat -i <input> -o <output> [-r <radius>] [-sF <sigmaF>] [-sX <sigmaX>]
```

### Create a distributed column matrix ###
Given a matrix representation in ABC format, the matrix need to be converted into distributed matrix slices with respect to the implementation of the matrix slice:

```
#!Shell

mr-mcl abc -i <input> -o <output> [-n] [-nsub <nsub>] [-te <te>] [--matrix-slice] [-vint] [-s <scale>] [--local] [-cm] [-co] [-zk] [-d] [-v]
```

#### Initialization parameters for the distributed matrix ####
* -n: use NativeCSCSlice.class as the matrix slice implementation instead of the standard Java implementation (CSCslice.class)
* -nsub: number of columns inside one matrix slice (default: 128)
* -te: number of partitions to split the data into, which defines the number of tasks for the following algorithms
* --matrix-slice: specify matrix slice class (default: CSCslice.class)
* -vint: use variable length integer encoding for non native slice implementations

#### Other parameters####
* -s: number of duplicates of the input matrix along the diagonal for benchmark purposes (default: 1)
* --local: run in MapReduce local mode inside the client JVM without YARN
* -cm: activate map output compress using LZ4-Codec
* -co: activate job output compress using LZ4-Codec
* -zk: start embedded ZooKeeer server
* -d: activate DEBUG logging level for this project
* -v: active INFO logging level for Hadoop

### Run the algorithm ###
The actual clustering algorithm running on the distributed slice matrix in HDFS/S3. The MCL and R-MCL algorithm are triggered using the command **bmcl** with a balance parameter of 1.0 or 0.0 respectively. Calling
```
#!Shell

mr-mcl bmcl -i <input> -o <output> [-a] [-I <inflation>] [-P <1/cutoff> | -p <cutoff>] [-S <select>] [-b <balance>] [-c <counters>] [-l <log>] [--stats] [--local] [-cm] [-co] [-zk] [-d] [-v]
```
generates a text file in the output folder, where each line contains a TAB delimited list of indices belonging to the same cluster.

#### Algorithm parameters ####
* -b: rate of updating the block transpose (default: 1.0)
* -a: use auto-prune instead of threshold prune
* -I: inflation parameter (default: 2.0)
* -P: 1/cutoff (default: 10000)
* -p: cutoff (default: 1.0E-4)
* -S: select (default: 50)

#### Other parameters####
* -c: path of file to write counters to (default: none)
* -l: path of file to write algorithm output to (default: none)
* --stats: show additional stats
* --local: run in MapReduce local mode inside the client JVM without YARN
* -cm: activate map output compress using LZ4-Codec
* -co: activate job output compress using LZ4-Codec
* -zk: start embedded ZooKeeer server
* -d: activate DEBUG logging level for this project
* -v: active INFO logging level for Hadoop

### Render an image of the clustering ###
To render an output of the clustering call
```
#!Shell

mr-mcl result -i <source> -c <clustering> -o <output> [-f <format>] [-lc <linecolour>] [--component <component>] [--imin <imin>] [--imax <imax>]
```
with following parameters:

* -i: source image file, folder of source images or .mat file
* -c: clustering file (output from the algorithm)
* -o: output folder
* -f: output format (default: jpg)
* -lc: gray scale line colour if the rendered clusters (default: 1.0)
* --component: for .mat files the component to render (default: I)
* --imin: for .mat files min threshold to render pixel value (default: 0.0)
* --imax: for .mat files maX threshold to render pixel value (default: 1.0)

## Project structure ##

The main algorithm is implemented by *mapred.alg.BMCLAlgortihm* extending *mapred.alg.AbstractMCLAlgorithm*.
The transpose and mcl job are implemented by *mapred.job.TransposeJob* and *mapred.job.MCLStep* respectively extending *mapred.job.AbstractMCLJob*.
The matrix slice implementations providing methods for serialization and oparations on the slice matrix must extend *io.writables.MCLMatrixSlice*. *io.writables.CSCslice* and *io.writables.nat.NativeCSCSlice* (with native code in src/main/native/src/io/writables/nat) are the provided matrix slice implementations in this project.

## Contact ##

This project is part of the diploma thesis "Large Scale Image Segmentation Using Markov Flows" of @miracoli at the Technical University of Berlin.

//TODO license