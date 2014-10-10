package io.cluster;

import java.awt.Point;

public interface ImageCluster extends Cluster<Point> {
	
	public boolean isBoundary(Point p);
	
	public ImageCluster getNeighbouringCluster(Point p);
	
	public ImageCluster not();
	
}