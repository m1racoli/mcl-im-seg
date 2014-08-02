/**
 * 
 */
package io.writables;

/**
 * @author Cedrik
 *
 */
public interface Metric<V> {
	
	/**
	 * distance between two features
	 * 
	 * @param v1
	 * @param v2
	 * @return
	 */
	public double dist(V v1, V v2);

}
