/**
 * 
 */
package zookeeper;

import org.apache.hadoop.io.Writable;

/**
 * @author Cedrik
 *
 */
public interface DistributedMetric<V> extends Writable {

	public void merge(V v);
	
	public void clear();
	
}
