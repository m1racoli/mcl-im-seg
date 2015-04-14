/**
 * 
 */
package io.cluster;

import java.awt.Point;
import java.util.Set;

/**
 * an image clustering is a clustering of imageclusters
 * 
 * @author Cedrik
 *
 */
public interface ImageClustering extends Set<ImageCluster> {
	
	public ImageCluster getCluster(Point e);
	
}
