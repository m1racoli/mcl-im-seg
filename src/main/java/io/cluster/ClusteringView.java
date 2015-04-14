/**
 * 
 */
package io.cluster;

import iterators.IteratorView;

import java.util.AbstractSet;
import java.util.Iterator;
import transform.Transformation;

/**
 * given a transformation K->V, a clustering view represents a clustering of type V backed by a clustering of type K.
 * 
 * @author Cedrik
 *
 */
public class ClusteringView<K,V> extends AbstractSet<Cluster<V>> implements Clustering<V> {

	private final Clustering<K> instance;
	private final Transformation<K, V> transformation;
	
	public ClusteringView(Clustering<K> instance, Transformation<K, V> transformation){
		this.instance = instance;
		this.transformation = transformation;
	}
	
	@Override
	public int size() {
		return instance.size();
	}
	
	@Override
	public Iterator<Cluster<V>> iterator() {
		return new IteratorView<Cluster<K>, Cluster<V>>(instance.iterator()) {

			@Override
			public Cluster<V> get(Cluster<K> val) {
				return new ClusterView(val);
			}
		};
	}
	
	@Override
	public Cluster<V> getCluster(V e) {
		return new ClusterView(instance.getCluster(transformation.inv(e)));
	}
	
	private final class ClusterView extends AbstractSet<V> implements Cluster<V> {

		private final Cluster<K> inst;
		
		public ClusterView(Cluster<K> inst) {
			this.inst = inst;
		}
		
		@Override
		public Iterator<V> iterator() {
			return new IteratorView<K, V>(inst.iterator()) {

				@Override
				public V get(K val) {
					return transformation.get(val);
				}
			};
		}

		@Override
		public int size() {
			return inst.size();
		}
		
		@Override
		public boolean contains(Object o) {
			return inst.contains(transformation.inv(o));
		}

		@Override
		public Cluster<V> not() {
			return new ClusterView(inst.not());
		}
		
	}	
	
}
