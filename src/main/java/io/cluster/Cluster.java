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
	
	@Override
	public boolean contains(Object o);
	
}
