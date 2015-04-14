package iterators;

import java.util.Iterator;

/**
 * an iterator over an array of ints
 * 
 * @author Cedrik
 *
 */
public class IntArrayIterator extends ReadOnlyIterator<Integer> implements Iterator<Integer> {

	private final int[] ar;
	private final int t;
	private int i;
		
	public IntArrayIterator(int[] ar) {
		this(ar,0,ar.length);
	}
	
	public IntArrayIterator(int[] ar, int s, int t) {
		this.ar = ar;
		this.i = s;
		this.t = t;
	}
	
	@Override
	public boolean hasNext() {
		return i < t;
	}

	@Override
	public Integer next() {
		return ar[i++];
	}

}
