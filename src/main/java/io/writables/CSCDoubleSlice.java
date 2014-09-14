/**
 * 
 */
package io.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import mapred.Counters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.ReadOnlyIterator;

/**
 * @author Cedrik Neumann
 *
 */
public final class CSCDoubleSlice extends DoubleMatrixSlice<CSCDoubleSlice> {

	private static final Logger logger = LoggerFactory.getLogger(CSCDoubleSlice.class);
	
	private double[] val = null;
	private long[] rowInd = null;
	private int[] colPtr = null;
	
	private boolean top_aligned = true;
	private transient SubBlockView view = null;

	public CSCDoubleSlice(){}
	
	public CSCDoubleSlice(Configuration conf){
		setConf(conf);
	}
	
	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		val = new double[max_nnz];
		rowInd = new long[max_nnz];
		colPtr = new int[nsub+1];
	}

	@Override
	public void clear(){
		Arrays.fill(colPtr, 0);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		top_aligned = true;
		
		for(int col = 0, l = colPtr.length; col < l; ++col){
			colPtr[col] = readInt(in);
		}
		
		for(int i = colPtr[nsub] - 1, s = colPtr[0]; i >= s; --i){
			val[i] = in.readDouble();
			rowInd[i] = readLong(in);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		
		if(view != null) {
			
			writeInt(out, 0);
			
			for(int col = 0, l = nsub, size = 0; col < l; ++col){
				size += view.size[col];
				writeInt(out, size);
			}
			
			long row_shift = view.row_shift;
			
			for(int col = nsub - 1; col >= 0; --col) {
				int size = view.size[col];
				if(size == 0) continue;
				view.size[col] = 0;
				for(int i = view.offset[col] - 1, s = i - size; i > s; --i){
					out.writeDouble(val[i]);
					writeLong(out, rowInd[i] + row_shift);
				}
			}

			return;
		}
		
		for(int col = 0, l = colPtr.length, off = -colPtr[0]; col < l; ++col){
			writeInt(out, colPtr[col] + off);
		}
		
		for(int i = colPtr[nsub] - 1, s = colPtr[0]; i >= s; --i){
			out.writeDouble(val[i]);
			writeLong(out, rowInd[i]);
		}
		
	}
	
	@Override
	public int fill(Iterable<SliceEntry> entries) {
		top_aligned = true;
		int current_col = 0;
		long last_row = -1;
		int l = 0;
		colPtr[0] = 0;
		int kmax = 0;
		int cs = 0;
		int max_nnz = this.max_nnz;
		
		for(SliceEntry entry : entries){
			
			if(l == max_nnz) {
				throw new IllegalArgumentException(String.format("matrix already full with max nnz = %d. Please specify proper k_max value",max_nnz));
			}
			
			if(current_col > entry.col) {
				throw new IllegalArgumentException(String.format("wrong column order: %d > %d",current_col,entry.col));
			}
			
			if(current_col < entry.col){
				
				arrayfill(colPtr, current_col + 1, entry.col + 1, l);
				
				kmax = Math.max(kmax, l-cs);
				last_row = -1;				
				current_col = entry.col;
				cs = colPtr[current_col];
			}			
			
			if(last_row >= entry.row) {
				throw new IllegalArgumentException(String.format("wrong row order in column %d: %d >= %d",current_col,last_row,entry.row));
			}
			
			val[l] = entry.val;
			rowInd[l++] = entry.row;
			last_row = entry.row;
		}
		
		arrayfill(colPtr, current_col + 1, colPtr.length, l);
		
		kmax = Math.max(kmax, l-cs);
		
//		if(logger.isDebugEnabled()){
//			logger.debug("colPtr: {}",Arrays.toString(colPtr));
//			logger.debug("rowInd: {}",Arrays.toString(Arrays.copyOf(rowInd, l)));
//			logger.debug("   val: {}",Arrays.toString(Arrays.copyOf(val, l)));
//		}
		
		return kmax;
	}
	
	private static void arrayfill(int[] a, int fromIndex, int toIndex, int val) {
        for (int i = fromIndex; i < toIndex; i++)
            a[i] = val;
    }

	@Override
	public void add(CSCDoubleSlice m) {
		
		if(top_aligned) {
			
			int end = val.length;
			
			for(int col_start = nsub - 1, col_end = nsub; col_start >= 0; col_end = col_start--) {
				int filled = addBack(colPtr[col_start], colPtr[col_end], m, m.colPtr[col_start], m.colPtr[col_end], end);
				colPtr[col_end] = end;
				end -= filled;
			}
			
			colPtr[0] = end;
			
		} else {
			
			int start = 0;
			
			for(int col_start = 0, col_end = 1, end = nsub; col_start < end; col_start = col_end++) {
				int filled = addForw(colPtr[col_start], colPtr[col_end], m, m.colPtr[col_start], m.colPtr[col_end], start);
				colPtr[col_start] = start;
				start += filled;
			}
			
			colPtr[nsub] = start;
		}
		
		top_aligned = !top_aligned;
	}

	@Override
	public CSCDoubleSlice multipliedBy(CSCDoubleSlice m, TaskAttemptContext context) {

		assert top_aligned : "right matrix is not correctly aligned";
		assert m.top_aligned : "align left";
		
		final double[] tmp_val = new double[kmax];
		final long[] tmp_rowInd = new long[kmax];
		
		int tmp_end = val.length;
		
		for(int col_end = nsub, col_start = col_end - 1; col_start >= 0; col_end = col_start--) {
			
			final int cs = colPtr[col_start];
			final int ct = colPtr[col_end];
			final int k = ct-cs;
			
			colPtr[col_end] = tmp_end;
			
			if(k == 0){
				continue;
			}
			
			System.arraycopy(rowInd, cs, tmp_rowInd, 0, k);
			System.arraycopy(val, cs, tmp_val, 0, k);
			boolean tmp_top_aligned = true;
			int filled = 0;
			
			for(int i = k - 1; i >= 0; i--) {
				
				final double factor = tmp_val[i];
				final int target_col = (int) tmp_rowInd[i]; //this is gonna be normalized to column range (< n_sub)
				
				final int target_cs = m.colPtr[target_col];
				final int target_ct = m.colPtr[target_col + 1];
				
				if(tmp_top_aligned){
					filled = addMultBack(cs, cs + filled, m, target_cs, target_ct, tmp_end, factor);
				} else {
					filled = addMultForw(tmp_end - filled, tmp_end, m, target_cs, target_ct, cs, factor);
				}
				
				tmp_top_aligned = !tmp_top_aligned;
			}
			
			tmp_end -= filled;
			
			if(tmp_top_aligned){
				System.arraycopy(val, cs, val, tmp_end, filled);
				System.arraycopy(rowInd, cs, rowInd, tmp_end, filled);
			}
		}
		
		colPtr[0] = tmp_end;		
		top_aligned = false;
		
		return this;
	}

	private final int addForw(int s1, int t1, CSCDoubleSlice m, int s2, int t2, int pos) {
		return addMultForw(s1, t1, m, s2, t2, pos, 1.0);
	}
	
	private final int addMultForw(int s1, int t1, CSCDoubleSlice m, int s2, int t2, int pos, double factor) {
		
//		if(logger.isDebugEnabled())
//			logger.debug("addMultForw(s1: {}, t1: {}, s2: {}, t2: {}, pos: {}, factor: {})",s,t,src_s,src_t,new_pos,factor);
		
		int p = pos;
		int i = s1, j = s2;
		
		while (i < t1 && j < t2) {			
			long row = rowInd[i];
			long src_Row = m.rowInd[j];
			
			if (row == src_Row) {
				rowInd[p] = row;
				val[p++] = factor * m.val[j++] + val[i++];
			} else {
				if (row < src_Row) {
					rowInd[p] = row;
					val[p++] = val[i++];	
				} else {
					rowInd[p] = src_Row;
					val[p++] = factor * m.val[j++];
				}
			}
		}
		
		if(i < t1) {			
			rowInd[p] = rowInd[i];
			val[p++] = val[i++];
			
			while (i < t1) {
				rowInd[p] = rowInd[i];
				val[p++] = val[i++];
			}
		} else if (j < t2){
			rowInd[p] = m.rowInd[j];
			val[p++] = factor * m.val[j++];
			
			while(j < t2) {
				rowInd[p] = m.rowInd[j];
				val[p++] = factor * m.val[j++];
			}
		}
		
		return p - pos;
	}
	
	private final int addBack(int s1, int t1, CSCDoubleSlice m, int s2, int t2, int pos) {
		return addMultBack(s1, t1, m, s2, t2, pos, 1.0);
	}
	
	private final int addMultBack(int s1, int t1, CSCDoubleSlice m, int s2, int t2, int pos, double factor) {
		
		int p = pos, i1 = t1 - 1, i2 = t2 - 1;
		while (i1 >= s1 && i2 >= s2) {	
			long r1 = rowInd[i1];
			long r2 = m.rowInd[i2];
			
			if (r1 == r2) {
				rowInd[--p] = r1;
				val[p] = val[i1--] + factor * m.val[i2--];
			} else {
				if (r1 > r2) {
					rowInd[--p] = r1;
					val[p] = val[i1--];
				} else {
					rowInd[--p] = r2;
					val[p] = factor * m.val[i2--];
				}
			}
		}
			
		while (i1 >= s1) {
			rowInd[--p] = rowInd[i1];
			val[p] = val[i1--];
		}			
		
		while (i2 >= s2) {
			rowInd[--p] = m.rowInd[i2];
			val[p] = factor * m.val[i2--];	
		}
		
		return pos - p;
	}
	
	@Override
	public int inflateAndPrune(TaskAttemptContext context) {
		
		final int[] selection = new int[kmax];
		int valPtr = 0;
		int max_s = 0;
		
		inflate(val, colPtr[0], colPtr[nsub]);
		
		for(int col_start = 0, col_end = 1, end = nsub; col_start < end; col_start = col_end++) {

			final int cs = colPtr[col_start];
			final int ct = colPtr[col_end];
			final int k = ct-cs;
			
			colPtr[col_start] = valPtr;
			
			switch(k){
			case 0:
				if(context != null) context.getCounter(Counters.EMPTY_COLUMNS).increment(1);
				continue;
			case 1:
				if(context != null) {
					context.getCounter(Counters.HOMOGENEOUS_COLUMNS).increment(1);
					context.getCounter(Counters.ATTRACTORS).increment(1);
				}
				
				rowInd[valPtr] = rowInd[cs];
				val[valPtr++] = 1.0f;
				if(max_s < 1) max_s = 1;
				continue;
			default:
				break;
			}
			
			int selected = prune(val, cs, ct, selection, context);

			
			for(int i  = 0; i < selected; i++){
				final int idx = selection[i];
				rowInd[valPtr] = rowInd[idx];
				val[valPtr++] = val[idx];
			}
			
			if(max_s < selected) max_s = selected;
			
		}
		
		colPtr[nsub] = valPtr;
		if(context != null) context.getCounter(Counters.NNZ).increment(size());
		return max_s;
	}
	
	@Override
	protected ReadOnlyIterator<CSCDoubleSlice> getSubBlockIterator(SliceId id) {
		
		assert top_aligned;
		
		return new SubBlockIterator(id);
	}

	@Override
	public int size() {
		return colPtr[nsub] - colPtr[0];
	}

	private final class SubBlockIterator extends ReadOnlyIterator<CSCDoubleSlice>{
		private final int nsub = CSCDoubleSlice.this.nsub;
		private final SliceId id;
		private final Queue<SubBlockSlice> queue = new PriorityQueue<CSCDoubleSlice.SubBlockSlice>(nsub);
		private final List<SubBlockSlice> list = new ArrayList<CSCDoubleSlice.SubBlockSlice>(nsub);
		private final int[] offset = new int[nsub];
		
		public SubBlockIterator(SliceId sliceId) {
			this.id = sliceId;
			
			if(view == null) {
				view = new SubBlockView();
			}
			
			System.arraycopy(colPtr, 0, offset, 0, nsub);			
			
			for(int column = 0, end = nsub; column < end; column++) {
				fetch(column);
			}
		}
		
		@Override
		public boolean hasNext() {
			return !queue.isEmpty();
		}
	
		@Override
		public CSCDoubleSlice next() {
			
			final SubBlockSlice first = queue.remove();
			list.add(first);
			
			while(queue.peek() != null && first.id == queue.peek().id) {
				list.add(queue.poll());
			}			
			
			for(SubBlockSlice slice : list) {
				int col = slice.column;
				view.offset[col] = offset[col];
				view.size[col] = slice.size;
				fetch(col);
			}
						
			list.clear();
			
			id.set(first.id);
			view.row_shift = - first.id*nsub;
			
			return CSCDoubleSlice.this;
		}
		
		private void fetch(int column) {
			
			int s = offset[column], t = colPtr[column + 1];
			
			if(s == t) {
				return;
			}
			
			int id = (int) (rowInd[s] / nsub), i = s + 1;
			long end = (long) (id + 1) * (long) nsub;
			
			while(i < t && rowInd[i] < end){
				i++;
			}
			
			queue.add(new SubBlockSlice(column, i - s, id));
			offset[column] = i;
		}
	}
	
	private static final class SubBlockSlice implements Comparable<SubBlockSlice> {

		final int column;
		final int size;
		final int id;
		
		public SubBlockSlice(int column, int size, int id) {
			this.column = column;
			this.size = size;
			this.id = id;
		}
		
		@Override
		public int compareTo(SubBlockSlice o) {
			int cmp = id == o.id ? 0 : id < o.id ? -1 : 1;
			if(cmp != 0) return cmp;
			return column < o.column ? -1 : 1;
		}		
	}
	
	private final class SubBlockView {
		long row_shift = 0;
		final int[] size = new int[nsub];
		final int[] offset = new int[nsub];
	}

	@Override
	public Iterable<SliceEntry> dump() {
		return new Iterable<SliceEntry>() {

			@Override
			public Iterator<io.writables.SliceEntry> iterator() {
				return new EntryIterator();
			}
			
		};
	}
	
	private final class EntryIterator extends ReadOnlyIterator<SliceEntry> {

		private final SliceEntry entry = new SliceEntry();
		private final int l = colPtr[nsub];
		private int col_end = colPtr[1];
		private int col = 0;
		private int i = colPtr[0];
		
		@Override
		public boolean hasNext() {
			return i < l;
		}

		@Override
		public SliceEntry next() {

			while(i == col_end) {
				col_end = colPtr[++col + 1];
			}
			
			entry.col = col;
			entry.row = rowInd[i];
			entry.val = (float) val[i++];
			
			return entry;
		}
		
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof CSCDoubleSlice){
			CSCDoubleSlice o = (CSCDoubleSlice) obj;

			for(int col = 0; col < colPtr.length-1; col++){
				final int s1 = colPtr[col];
				final int t1 = colPtr[col+1];
				final int s2 = o.colPtr[col];
				if(t1-s1 != o.colPtr[col+1]-s2){
					return false;
				}
				for(int i1 = s1, i2 = s2; i1 < t1; i1++, i2++){
					if(val[i1] != o.val[i2] || rowInd[i1] != o.rowInd[i2]){
						return false;
					}
				}
			}
			return true;
		}
		return false;
	}

	@Override
	public float makeStochastic(TaskAttemptContext context) {
		
		float chaos = 0.0f;
		
		for(int col_start = 0, col_end = 1, end = nsub; col_start < end; col_start = col_end++) {
			chaos = Math.max(chaos, normalize(val, colPtr[col_start], colPtr[col_end], context));
		}
		
		return chaos;
	}

	@Override
	public void addLoops(SliceIndex id) {
		// TODO Auto-generated method stub
		
	}

}
