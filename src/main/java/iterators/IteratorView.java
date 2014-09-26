/**
 * 
 */
package iterators;

import java.util.Iterator;

/**
 * @author Cedrik
 *
 */
public abstract class IteratorView<K, V> implements Iterator<V> {

	private final Iterator<K> iter;
	
	public IteratorView(Iterator<K> iter) {
		this.iter = iter;
	}
	
	public abstract V transform(K val);
	
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
		return transform(iter.next());
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		iter.remove();
	}

}
