package iterators;

import java.util.Iterator;

/**
 * this class helps to iterate over an array of iterators of the same type
 * 
 * @author Cedrik
 *
 * @param <E>
 */
public class ConcatenatedIterator<E> implements Iterator<E> {

	private final Iterator<E>[] its;
	private Iterator<E> current = null;
	private Iterator<E> last = null;
	private int i = 0;	

	@SafeVarargs
	public ConcatenatedIterator(Iterator<E> ... its) {
		this.its = its;
	}
	
	@Override
	public boolean hasNext() {
		if(current == null){
			current = its[0];
		}
		
		while(!current.hasNext() && i < its.length){
			current = its[i++];
		}
		
		return i < its.length;
	}

	@Override
	public E next() {
		last = current;
		return current.next();
	}

	@Override
	public void remove() {
		last.remove();
	}

}
