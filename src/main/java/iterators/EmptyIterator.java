/**
 * 
 */
package iterators;

/**
 * an iterator which is empty
 * 
 * @author Cedrik
 *
 */
public final class EmptyIterator<E> extends ReadOnlyIterator<E> {

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public E next() {
		return null;
	}

}
