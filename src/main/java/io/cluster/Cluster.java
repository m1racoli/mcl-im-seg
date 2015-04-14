/**
 * 
 */
package io.cluster;

import java.util.Set;

/**
 * a set of items of type E which is refered to as cluster.
 * 
 * @author Cedrik
 *
 */
public interface Cluster<E> extends Set<E>{
	
	public Cluster<E> not();
	
}
