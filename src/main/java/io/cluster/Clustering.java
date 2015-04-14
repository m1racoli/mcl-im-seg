/**
 * 
 */
package io.cluster;

import java.util.Set;

/**
 * a set of clusters of type E which is refered to as clustering.
 * 
 * @author Cedrik
 *
 */
public interface Clustering<E> extends Set<Cluster<E>> {

	public Cluster<E> getCluster(E e);
	
}
