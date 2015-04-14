package io.cluster;

import java.awt.Point;

/**
 * an imagecluster is a cluster of type Point
 * 
 * @author Cedrik
 *
 */
public interface ImageCluster extends Cluster<Point> {
	
	public boolean isBoundary(Point p);
	
	public ImageCluster getNeighbouringCluster(Point p);
	
	public ImageCluster not();
	
}