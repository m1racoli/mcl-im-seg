/**
 * 
 */
package iterators;

import java.util.Iterator;

/**
 * @author Cedrik
 *
 */
public abstract class ReadOnlyIterator<E> implements Iterator<E> {

	/* (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public final void remove() {
		throw new UnsupportedOperationException("remove is not supported");
	}

}
