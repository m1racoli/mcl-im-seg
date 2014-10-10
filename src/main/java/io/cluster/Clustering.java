/**
 * 
 */
package io.cluster;

import java.util.Set;

/**
 * @author Cedrik
 *
 */
public interface Clustering<E> extends Set<Cluster<E>> {

	public Cluster<E> getCluster(E e);
	
}
