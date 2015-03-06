/**
 * 
 */
package iterators;

/**
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
