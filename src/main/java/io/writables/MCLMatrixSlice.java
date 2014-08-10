/**
 * 
 */
package io.writables;

import java.util.Iterator;

import mapred.MCLContext;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import util.ReadOnlyIterator;

/**
 * @author Cedrik
 *
 */
public abstract class MCLMatrixSlice<V extends MCLMatrixSlice<V>> extends MCLContext implements Writable {
	
	protected final int init_nnz;
	protected final int n_sub;
	
	MCLMatrixSlice(int init_nnz){
		this.init_nnz = init_nnz;
		this.n_sub = getNSub();
	}
	
	/**
	 *  clear contents
	 */
	public abstract void clear();
	
	/**
	 * add values ordered by (col,row) ascending
	 * @param col column of current value
	 * @param row row of current value
	 * @param values
	 */
	public abstract void add(IntWritable col, LongWritable row, Iterable<Float> values);

	/** 
	 * @param m to add to this
	 */
	public abstract void add(final V m);
	
	//TODO column -> slice matcher
	
	/**
	 * @param subBlock to multiply with
	 * @return product with subBlock
	 */
	public abstract V getProduct(final V subBlock);
	
	/**
	 * @param id to write index of current sub block to
	 * @return iterator over the sub blocks
	 */
	protected abstract ReadOnlyIterator<V> getSubBlockIterator(final SliceId id);
	
	/** 
	 * @return nnz
	 */
	public abstract int size();
	
	/**
	 * inflate, prune and normalize
	 */
	public abstract void inflateAndPrune(TaskInputOutputContext<?, ?, ?, ?> context);
	
	public final boolean isEmpty(){
		return 0 == size();
	}

	/**
	 * iterate over subBlocks and write current index to id
	 * @param id to write current index to
	 * @return
	 */
	public final Iterable<V> getSubBlocks(final SliceId id){
		return new Iterable<V>() {
			@Override
			public Iterator<V> iterator() {
				return getSubBlockIterator(id);
			}
		};
	}
}
