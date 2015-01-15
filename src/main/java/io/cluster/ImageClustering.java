/**
 * 
 */
package io.cluster;

import java.awt.Point;
import java.util.Set;

/**
 * @author Cedrik
 *
 */
public interface ImageClustering extends Set<ImageCluster> {
	
	public ImageCluster getCluster(Point e);
	
}
