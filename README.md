# Large Scale Image Segmentation Using Markov Flows #

This repository contains an implementation of the Markov Clustering (MCL) Algorithm and Regularized Markov Clustering (R-MCL) Algorithm using the Hadoop MapReduce framework and utilities to apply those algorithms to image segmentation.

### Requirements ###

* Linux (although not tested on Mac OS X)
* Java 7+, 64-bit
* Hadoop 2.4+
* Maven 3+ (for building)
* Cmake and gcc (for building native)

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
The whole procedure of segmenting an image consists of four steps.

### Extract Pixel similarities ###

### Create a distributed column matrix ###

### Run the algorithm ###

### Render an image of the clustering ###


### Contribution guidelines ###

* Writing tests
* Code review
* Other guidelines

### Contact ###

This project is part of the diploma thesis "Large Scale Image Segmentation Using Markov Flows" of @miracoli at the Technical University of Berlin.

//TODO license