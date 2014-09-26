/**
 * 
 */
package io.cluster;

import iterators.IteratorView;
import java.util.AbstractSet;
import java.util.Iterator;

/**
 * @author Cedrik
 *
 */
public abstract class ClusterSetView<K,V> extends AbstractSet<Cluster<V>> implements ClusterSet<V> {

	private final ClusterSet<K> instance;
	
	public ClusterSetView(ClusterSet<K> instance){
		this.instance = instance;
	}
	
	public abstract V forw(K src);
	
	public abstract K back(Object dest);
	
	@Override
	public int size() {
		return instance.size();
	}
	
	@Override
	public Iterator<Cluster<V>> iterator() {
		return new IteratorView<Cluster<K>, Cluster<V>>(instance.iterator()) {

			@Override
			public Cluster<V> transform(Cluster<K> val) {
				return new ClusterView(val);
			}
		};
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
				public V transform(K val) {
					return forw(val);
				}
			};
		}

		@Override
		public int size() {
			return inst.size();
		}
		
		@Override
		public boolean contains(Object o) {
			return inst.contains(back(o));
		}
		
	}
	
	
}
