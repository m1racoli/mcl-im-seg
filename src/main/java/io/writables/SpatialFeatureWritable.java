/**
 * 
 */
package io.writables;

import java.awt.Point;

/**
 * feature writable which has a point in a 2d dimensional space, like a pixel in an image.
 * 
 * @author Cedrik
 *
 */
public interface SpatialFeatureWritable<V extends FeatureWritable<V>> extends FeatureWritable<V>{

	public Point getPosition();
	
}
