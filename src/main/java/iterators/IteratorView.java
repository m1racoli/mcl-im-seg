/**
 * 
 */
package iterators;

import java.util.Iterator;

/**
 * an interatorview provides an iterator of type V backed by an iterator of type E, where a transformation E->V is provided
 * 
 * @author Cedrik
 *
 */
public abstract class IteratorView<K, V> implements Iterator<V> {

	private final Iterator<K> iter;
	
	public IteratorView(Iterator<K> iter) {
		this.iter = iter;
	}
	
	public abstract V get(K val);
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public V next() {
		return get(iter.next());
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		iter.remove();
	}

}
