/**
 * 
 */
package io.writables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import mapred.MCLConfigHelper;
import mapred.MCLDefaults;
import mapred.MCLInstance;
import mapred.PrintMatrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.beust.jcommander.IStringConverter;

import util.ReadOnlyIterator;

/**
 * @author Cedrik
 *
 */
public abstract class MCLMatrixSlice<M extends MCLMatrixSlice<M>> extends MCLInstance implements Writable {
	
	protected double inflation = MCLDefaults.inflation;
	protected float cutoff = MCLDefaults.cutoff;
	protected int selection = MCLDefaults.selection;
	protected PrintMatrix print_matrix = MCLDefaults.printMatrix;
	protected int max_nnz = kmax * nsub;
	
	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		inflation = MCLConfigHelper.getInflation(conf);
		cutoff = Math.max(MCLConfigHelper.getCutoff(conf),1.0f/MCLConfigHelper.getCutoffInv(conf));
		selection = MCLConfigHelper.getSelection(conf);
		print_matrix = MCLConfigHelper.getPrintMatrix(conf);
		max_nnz = kmax * nsub;
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
	 * @return kmax
	 */
	public abstract int fill(Iterable<MatrixEntry> entries);	
	
	public void fill(int[] col, long[] row, float[] val) {
		
		int l = col.length;
		
		if(l != row.length || l != val.length) {
			throw new IllegalArgumentException("dimension missmatch of input arrays col,row,val");
		}
		
		List<MatrixEntry> entries = new ArrayList<MatrixEntry>(l);
		for(int i = 0; i < l; i++) {
			entries.add(MatrixEntry.get(col[i], row[i], val[i]));
		}
		Collections.sort(entries);
		
		fill(entries);
	}

	public abstract Iterable<MatrixEntry> dump();
	
	/** 
	 * @param m to add to this
	 */
	public abstract void add(final M m);
	
	//TODO column -> slice matcher
	
	/**
	 * @param M to multiply with
	 * @return this multiplied by m
	 */
	public abstract M multipliedBy(final M m, final TaskAttemptContext context);
	
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
	public abstract int inflateAndPrune(TaskAttemptContext context);
	
	public final boolean isEmpty(){
		return 0 == size();
	}

	/**
	 * iterate over subBlocks and write current index to id
	 * @param id to write current index to
	 * @return
	 */
	public final Iterable<M> getSubBlocks(final SliceId id){
		return new Iterable<M>() {
			@Override
			public Iterator<M> iterator() {
				return getSubBlockIterator(id);
			}
		};
	}
	
	public static final class MatrixEntry implements Comparable<MatrixEntry> {
		public int col = 0;
		public long row = 0;
		public float val = 0;
		
		public static MatrixEntry get(int col, long row, float val) {
			MatrixEntry e = new MatrixEntry();
			e.col = col;
			e.row = row;
			e.val = val;
			return e;
		}
		
		@Override
		public int compareTo(MatrixEntry o) {
			int cmp = col == o.col ? 0 : col < o.col ? -1 : 1;
			if(cmp != 0) return cmp;
			return row == o.row ? 0 : row < o.row ? -1 : 1;
		}
		
		@Override
		public String toString() {
			return String.format("[c: %d, r: %d, v: %f]", col,row,val);
		}
	}
	
	@Override
	public String toString() {
		
		switch (print_matrix) {
		case ALL:
			
			float[] matrix = new float[(int) (n*nsub)];
			Arrays.fill(matrix, 0.0f);
			
			for(MatrixEntry e : dump()) {
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

		@SuppressWarnings("unchecked")
		@Override
		public Class<? extends MCLMatrixSlice<?>> convert(String str) {
			try {
				return (Class<? extends MCLMatrixSlice<?>>) Class.forName(str);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
