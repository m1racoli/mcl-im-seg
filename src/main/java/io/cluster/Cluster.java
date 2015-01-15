/**
 * 
 */
package io.cluster;

import java.util.Set;

/**
 * @author Cedrik
 *
 */
public interface Cluster<E> extends Set<E>{
	
	public Cluster<E> not();
	
}
