/**
 * 
 */
package io.cluster;

import transform.Transformation;

/**
 * an indexedclustering view is a clustering view which an clustering of type Integer as the backing clustering
 * 
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
