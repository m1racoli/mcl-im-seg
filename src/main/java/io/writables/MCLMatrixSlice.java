/**
 * 
 */
package io.writables;

import iterators.ReadOnlyIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import mapred.MCLConfigHelper;
import mapred.MCLDefaults;
import mapred.MCLInstance;
import mapred.MCLStats;
import mapred.PrintMatrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import com.beust.jcommander.IStringConverter;

/**
 * @author Cedrik
 *
 */
public abstract class MCLMatrixSlice<M extends MCLMatrixSlice<M>> extends MCLInstance implements Writable {
	
	protected double inflation = MCLDefaults.inflation;
	protected float cutoff = MCLDefaults.cutoff;
	protected int select = MCLDefaults.selection;
	protected PrintMatrix print_matrix = MCLDefaults.printMatrix;
	protected int max_nnz = kmax * nsub;
	protected boolean auto_prune = MCLDefaults.autoPrune;
	protected float pruneA = MCLDefaults.pruneA;
	protected float pruneB = MCLDefaults.pruneB;
	protected boolean javaQueue = MCLDefaults.javaQueue;
	
	/**
	 *  clear contents
	 */
	public abstract void clear();
	
	/**
	 * add values ordered by (col,row) ascending
	 * @param col column of current value
	 * @param row row of current value
	 * @param values
	 * @return kmax
	 */
	public abstract int fill(Iterable<SliceEntry> entries);	
	
	public abstract Iterable<SliceEntry> dump();
	
	/** 
	 * @param m to add to this
	 */
	public abstract void add(final M m);
	
	/**
	 * @param m to multiply with
	 * @return this = m * this
	 */
	public abstract M multipliedBy(M m);
	
	/**
	 * @param id to write index of current sub block to
	 * @return iterator over the sub blocks
	 */
	protected abstract ReadOnlyIterator<M> getSubBlockIterator(final SliceId id);
	
	/** 
	 * @return nnz
	 */
	public abstract int size();
	
	/**
	 * inflate, prune and normalize
	 * @return max column size
	 */
	public abstract void inflateAndPrune(MCLStats stats);
	
	/**
	 * @param context
	 * @return chaos
	 */
	public abstract void makeStochastic();
	
	/**
	 * equality test on implementation level
	 */
	public abstract boolean equals(Object obj);

	/**
	 * adds self loops to the matrix
	 * @param id
	 */
	public abstract void addLoops(SliceIndex id);
	
	/**
	 * get sum [ (x_ij-y_ij)^2 ] of this matrix and the other
	 * @param return prematurely if value is greater than this value
	 */
	public abstract double sumSquaredDifferences(M other);
	
	public final boolean isEmpty(){
		return 0 == size();
	}

	/**
	 * iterate over subBlocks and write current index to id
	 * @param id to write current index to
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public final Iterable<M> getSubBlocks(final SliceId id){
		
		if (n == nsub) {
			id.set(0);
			return Collections.<M>singleton((M) this);
		}
		
		return new Iterable<M>() {
			@Override
			public Iterator<M> iterator() {
				return getSubBlockIterator(id);
			}
		};
	}

	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		inflation = MCLConfigHelper.getInflation(conf);
		cutoff = MCLConfigHelper.getCutoff(conf);
		select = MCLConfigHelper.getSelection(conf);
		print_matrix = MCLConfigHelper.getPrintMatrix(conf);
		max_nnz = kmax * nsub;
		//TODO pruneA,pruneB,
		auto_prune = MCLConfigHelper.getAutoPrune(conf);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Configuration getConf() {
		Configuration conf = super.getConf();
		MCLConfigHelper.setInflation(conf, inflation);
		MCLConfigHelper.setCutoff(conf, cutoff);
		MCLConfigHelper.setSelection(conf, select);
		MCLConfigHelper.setPrintMatrix(conf, print_matrix);
		MCLConfigHelper.setAutoPrune(conf, auto_prune);
		MCLConfigHelper.setMatrixSliceClass(conf, (Class<? extends MCLMatrixSlice<?>>) getClass());
		return conf;
	}
	
	@Override
	public String toString() {
		
		switch (print_matrix) {
		case ALL:
			
			float[] matrix = new float[(int) (n*nsub)];
			Arrays.fill(matrix, 0.0f);
			
			for(SliceEntry e : dump()) {
				matrix[(int) (e.col + (nsub * e.row))] = e.val;
			}
			
			StringBuilder builder = new StringBuilder();
			for(int i = 0; i < n; i++) {
				int off = i*nsub;
				builder.append('|');
				for(int j = 0; j < nsub; j++) {
					float v = matrix[j + off];
					builder.append(v == 0.0f ? "     " : String.format(" %3.2f", v));
				}
				builder.append('|').append('\n');
			}
			
			return builder.toString();
		default:			
			int size = size();
			if(size == 0) return "[empty]";
			
			return String.format("[nnz: %d]", size);
		}
	}
	
	public static class ClassConverter implements IStringConverter<Class<? extends MCLMatrixSlice<?>>> {

		@Override
		public Class<? extends MCLMatrixSlice<?>> convert(String str) {
			return classFromString(str);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static final Class<? extends MCLMatrixSlice<?>> classFromString(String name){
		try {
			return (Class<? extends MCLMatrixSlice<?>>) Class.forName(name);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static final <M extends MCLMatrixSlice<M>> M getInstance(Configuration conf){
		return getMatrixSliceInstance(conf);
	}
	
	public final M getInstance() {
		return getInstance(getConf());
	}
	
	/**
	 * Creates a deep copy using Writable interface. Override for a direct implementation.
	 * 
	 * @return
	 * @throws IOException
	 */
	public M deepCopy()
	{		
		try
		{
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			write(new DataOutputStream(out));
			M o = getInstance();
			ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
			o.readFields(new DataInputStream(in));
			return o;
		} catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}
	
}
