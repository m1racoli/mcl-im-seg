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
public abstract class MCLMatrixSlice extends MCLContext implements Writable {
	
	protected final int init_nnz;
	
	MCLMatrixSlice(){
		this(getInitNnz());
	}
	
	MCLMatrixSlice(int init_nnz){
		this.init_nnz = init_nnz;
	}
	
	/**
	 *  clear contents
	 */
	public abstract void clear();
	
	/** 
	 * @param m to add to this
	 */
	protected abstract void add(final MCLMatrixSlice m);

	protected abstract void add(final LongWritable row, MCLMatrixSlice subBlock);
	
	public abstract void insert(long row, long col, float val);
	
	//TODO column -> slice matcher
	
	public final void combine(final LongWritable row, Iterable<MCLMatrixSlice> vals){
		clear();
		for(MCLMatrixSlice val : vals){
			add(row, val);
		}
	}
	
	/**
	 * @param subBlock to multiply with
	 * @return product with subBlock
	 */
	protected abstract MCLMatrixSlice product(final MCLMatrixSlice subBlock);
	
	/**
	 * @param id to write index of current sub block to
	 * @return iterator over the sub blocks
	 */
	protected abstract ReadOnlyIterator<MCLMatrixSlice> getSubBlockIterator(final SliceId id);

	/**
	 * clear and add the values
	 * @param values
	 */
	public final void combine(final Iterable<MCLMatrixSlice> values, TaskInputOutputContext<?, ?, ?, ?> context){
		clear();
		for(MCLMatrixSlice value : values)
			add(value);
	}
	
	public void combineAndProcess(final Iterable<MCLMatrixSlice> values, TaskInputOutputContext<?, ?, ?, ?> context){
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
	public final Iterable<MCLMatrixSlice> subBlocks(final SliceId id){
		return new Iterable<MCLMatrixSlice>() {
			@Override
			public Iterator<MCLMatrixSlice> iterator() {
				return getSubBlockIterator(id);
			}
		};
	}

	public void init(final Index idx, final Iterable<? extends Feature> pixels) {
		clear();
		construct(idx.row, null); //TODO
	}
	
	public abstract void construct(LongWritable row, Iterable<Float> values);
}
