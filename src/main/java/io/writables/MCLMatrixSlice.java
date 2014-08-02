/**
 * 
 */
package io.writables;

import java.util.Iterator;

import mapred.MCLContext;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import util.ReadOnlyIterator;

/**
 * @author Cedrik
 *
 */
@SuppressWarnings("rawtypes")
public abstract class MCLMatrixSlice<V extends MCLMatrixSlice,E extends Writable> extends MCLContext implements Writable {
	
	/**
	 *  clear contents
	 */
	public abstract void clear();
	
	/** 
	 * @param m to add to this
	 */
	protected abstract void add(final V m);

	protected abstract void add(final LongWritable row, E subBlock);
	
	//TODO column -> slice matcher
	
	public final void combine(final LongWritable row, Iterable<E> vals){
		clear();
		for(E val : vals){
			add(row, val);
		}
	}
	
	/**
	 * @param subBlock to multiply with
	 * @return product with subBlock
	 */
	protected abstract MCLMatrixSlice product(final E subBlock);
	
	/**
	 * @param id to write index of current sub block to
	 * @return iterator over the sub blocks
	 */
	protected abstract ReadOnlyIterator<E> getSubBlockIterator(final LongWritable id);

	/**
	 * clear and add the values
	 * @param values
	 */
	public final void combine(final Iterable<V> values, TaskInputOutputContext<?, ?, ?, ?> context){
		clear();
		for(V value : values)
			add(value);
	}
	
	public void combineAndProcess(final Iterable<V> values, TaskInputOutputContext<?, ?, ?, ?> context){
		combine(values, context);
		process(context);
	}
	
	public abstract int size();
	
	public final boolean isEmpty(){
		return 0 == size();
	}

	/**
	 * inflate, prune and normalize
	 */
	public abstract void process(TaskInputOutputContext<?, ?, ?, ?> context);
	
	/**
	 * iterate over subBlocks and write current index to id
	 * @param id to write current index to
	 * @return
	 */
	public final Iterable<E> subBlocks(final LongWritable id){
		return new Iterable<E>() {
			@Override
			public Iterator<E> iterator() {
				return getSubBlockIterator(id);
			}
		};
	}

	public final Iterable<MCLMatrixSlice> getProductsWith(final LongWritable id, final MCLMatrixSlice<V,E> m){
		return new Iterable<MCLMatrixSlice>() {
			@Override
			public Iterator<MCLMatrixSlice> iterator() {
				return new ProductIterator(id, m);
			}
		};
	}
	
	private final class ProductIterator extends ReadOnlyIterator<MCLMatrixSlice> {

		private final Iterator<E> iter;
		
		private ProductIterator(final LongWritable id, final MCLMatrixSlice<V,E> m){
			iter = m.getSubBlockIterator(id);
		}
		
		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public MCLMatrixSlice next() {
			return product(iter.next());
		}
	}
}
