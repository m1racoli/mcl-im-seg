/**
 * 
 */
package io.writables;

/**
 * @author Cedrik
 *
 */
public interface Feature<V extends Feature<V>> {

	public float dist(V feature);
	
	public V copy(V instance);
}
