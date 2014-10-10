/**
 * 
 */
package io.cluster;

import transform.Transformation;

/**
 * @author Cedrik
 *
 */
public class IndexedClusteringView<V> extends ClusteringView<Integer, V> {

	/**
	 * @param index
	 * @param transformation
	 */
	public IndexedClusteringView(Clustering<Integer> index,
			Transformation<Integer, V> transformation) {
		super(index, transformation);
	}

}
