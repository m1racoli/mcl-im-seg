/**
 * 
 */
package io.writables;

import org.apache.hadoop.io.Writable;

/**
 * @author Cedrik
 *
 */
public interface FeatureWritable<V> extends Feature<V>, Writable {
	
}
