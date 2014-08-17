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
import mapred.Selector;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.ReadOnlyIterator;

/**
 * @author Cedrik Neumann
 *
 */
public final class CSCSlice extends MCLMatrixSlice<CSCSlice> {

	private static final Logger logger = LoggerFactory.getLogger(CSCSlice.class);
	
	private final float[] val;
	private final long[] rowInd;
	private final int[] colPtr;
	
	private boolean top_aligned = true;
	private transient Selector selector = null;
	private transient SubBlockView view = null;

	public CSCSlice(){
		val = new float[max_nnz];
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
			val[i] = in.readFloat();
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
					out.writeFloat(val[i]);
					writeLong(out, rowInd[i] + row_shift);
				}
			}

			return;
		}
		
		for(int col = 0, l = colPtr.length, off = -colPtr[0]; col < l; ++col){
			writeInt(out, colPtr[col] + off);
		}
		
		for(int i = colPtr[nsub] - 1, s = colPtr[0]; i >= s; --i){
			out.writeFloat(val[i]);
			writeLong(out, rowInd[i]);
		}
		
	}
	
	@Override
	public int fill(Iterable<MatrixEntry> entries) {
		top_aligned = true;
		int current_col = 0;
		long last_row = -1;
		int l = 0;
		colPtr[0] = 0;
		float col_sum = 0.0f;
		int kmax = 0;
		int cs = 0;
		
		for(MatrixEntry entry : entries){
			
			if(l == max_nnz) {
				throw new IllegalArgumentException(String.format("matrix already full with max nnz = %d. Please specify proper k_max value",max_nnz));
			}
			
			if(current_col > entry.col) {
				throw new IllegalArgumentException(String.format("wrong column order: %d > %d",current_col,entry.col));
			}
			
			if(current_col < entry.col){
				
				arrayfill(colPtr, current_col + 1, entry.col + 1, l);
				
				for(int i = cs; i < l; i++) {
					val[i] /= col_sum;
				}
				kmax = Math.max(kmax, l-cs);

				col_sum = 0.0f;
				last_row = -1;				
				current_col = entry.col;
				cs = colPtr[current_col];
			}			
			
			if(last_row >= entry.row) {
				throw new IllegalArgumentException(String.format("wrong row order in column %d: %d >= %d",current_col,last_row,entry.row));
			}
			
			col_sum += entry.val;
			val[l] = entry.val;
			rowInd[l++] = entry.row;
			last_row = entry.row;
		}
		
		arrayfill(colPtr, current_col + 1, colPtr.length, l);
		
		for(int i = cs; i < l; i++) {
			val[i] /= col_sum;
		}
		
		kmax = Math.max(kmax, l-cs);
		
		if(logger.isDebugEnabled()){
			logger.debug("colPtr: {}",Arrays.toString(colPtr));
			logger.debug("rowInd: {}",Arrays.toString(Arrays.copyOf(rowInd, l)));
			logger.debug("   val: {}",Arrays.toString(Arrays.copyOf(val, l)));
		}
		
		return kmax;
	}
	
	private static void arrayfill(int[] a, int fromIndex, int toIndex, int val) {
        for (int i = fromIndex; i < toIndex; i++)
            a[i] = val;
    }

	@Override
	public void add(CSCSlice m) {
		
		if(top_aligned) {
			
			int tmp_end = val.length;
			
			for(int current_col = nsub - 1, next_col = nsub; current_col >= 0; next_col = current_col--) {
				colPtr[next_col] = tmp_end;
				tmp_end -= addBack(colPtr[current_col], colPtr[next_col], m, m.colPtr[current_col], m.colPtr[next_col], tmp_end);
			}
			
			colPtr[0] = tmp_end;
			
		} else {
			
			int tmp_start = 0;
			
			for(int current_col = 0, next_col = 1; current_col < nsub; current_col = next_col++) {
				colPtr[current_col] = tmp_start;
				tmp_start += addForw(colPtr[current_col], colPtr[next_col], m, m.colPtr[current_col], m.colPtr[next_col], tmp_start);
			}
			
			colPtr[nsub] = tmp_start;
		}
		
		top_aligned = !top_aligned;
	}

	@Override
	public CSCSlice multipliedBy(CSCSlice m, TaskAttemptContext context) {

		//we assume this is a sub block, for which there are maximal n_sub rows
		final float[] tmp_val = new float[nsub];
		final long[] tmp_rowInd = new long[nsub];
		
		int tmp_end = val.length;
		
		for(int current_col = nsub - 1; current_col >= 0; current_col--) {
			
			final int cs = colPtr[current_col];
			final int ct = colPtr[current_col+1];
			final int k = ct-cs;
			
			colPtr[current_col+1] = tmp_end;
			
			if(k == 0){
				continue;
			}
			
			System.arraycopy(rowInd, cs, tmp_rowInd, 0, k);
			System.arraycopy(val, cs, tmp_val, 0, k);
			boolean tmp_top_aligned = true;
			int filled = 0;
			
			for(int i = k - 1; i >= 0; i--) {
				
				final float factor = tmp_val[i];
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

	private final int addForw(int s, int t, CSCSlice src, int src_s, int src_t, int new_pos) {
		return addMultForw(s, src_t, src, src_s, src_t, new_pos, 1.0f);
	}
	
	private final int addMultForw(int s, int t, CSCSlice src, int src_s, int src_t, int new_pos, float factor) {
		int p = new_pos;
		int i = s, j = src_s;
		
		while (i < t && j < src_t) {			
			long row = rowInd[i];
			long src_Row = src.rowInd[j];
			
			if (row == src_Row) {
				rowInd[p] = row;
				val[p++] = factor * src.val[j++] + val[i++];
			} else {
				if (row < src_Row) {
					rowInd[p] = row;
					val[p++] = val[i++];	
				} else {
					rowInd[p] = src_Row;
					val[p++] = factor * src.val[j++];
				}
			}
		}
		
		if(i < t) {			
			rowInd[p] = rowInd[i];
			val[p++] = val[i++];
			
			while (i < t) {
				rowInd[p] = rowInd[i];
				val[p++] = val[i++];
			}
		} else if (j < src_t){
			rowInd[p] = src.rowInd[j];
			val[p++] = factor * src.val[j++];
			
			while(j < src_t) {
				rowInd[p] = src.rowInd[j];
				val[p++] = factor * src.val[j++];
			}
		}
		
		return p - new_pos;
	}
	
	private final int addBack(int s, int t, CSCSlice src, int src_s, int src_t, int new_pos) {
		return addMultBack(s, src_t, src, src_s, src_t, new_pos, 1.0f);
	}
	
	private final int addMultBack(int s, int t, CSCSlice src, int src_s, int src_t, int new_pos, float factor) {
		int p = new_pos;
		int i = t, j = src_t ;
		
		while (i > s && j > src_s) {			
			long row = rowInd[--i];
			long src_Row = src.rowInd[--j];
			
			if (row == src_Row) {				
				rowInd[--p] = row;
				val[p] = factor * src.val[j] + val[i];				
			} else {				
				if (row > src_Row) {					
					rowInd[--p] = row;
					val[p] = val[i];					
				} else {					
					rowInd[--p] = src_Row;
					val[p] = factor * src.val[j];
				}
			}
		}
		
		if (i > s) {			
			rowInd[--p] = rowInd[--i];
			val[p] = val[i];
			
			while (i > s) {
				rowInd[--p] = rowInd[--i];
				val[p] = val[i];
			}			
		} else if (j > src_s) {			
			rowInd[--p] = src.rowInd[--j];
			val[p] = factor * src.val[j];
			
			while (j > src_s) {				
				rowInd[--p] = src.rowInd[--j];
				val[p] = factor * src.val[j];				
			}
		}
		
		return new_pos - p;
	}
	
	@Override
	public int inflateAndPrune(TaskAttemptContext context) {
		
		top_aligned = !top_aligned;
		
		if(selector == null){
			selector = new Selector(); //TODO custom class
		}
		
		final int[] selection = new int[kmax];
		final int incr = top_aligned ? 1 : -1;
		final int colPtr_s = top_aligned ? 0 : colPtr.length - 2;
		final int colPtr_t = top_aligned ? colPtr.length - 2 : 0;
		int valPtr = top_aligned ? 0 : val.length - 1;
		final int prev_col_direction = top_aligned ? 0 : 1;
		int max_s = 0;
		
		//TODO two cases
		for(int currenct_col = colPtr_s; currenct_col != colPtr_t; currenct_col += incr) {

			final int cs = colPtr[currenct_col];
			final int ct = colPtr[currenct_col+1];
			final int k = ct-cs;
			
			colPtr[currenct_col+prev_col_direction] = valPtr+prev_col_direction;
			
			switch(k){
			case 0:
				if(context != null) context.getCounter(Counters.EMPTY_COLUMNS).increment(1);
				continue;
			case 1:
				if(val[cs] != 1.0f){
					//TODO remove for non debug
					if(context != null) context.getCounter(Counters.SINGLE_COLUMN_NOT_ONE).increment(1);
				}
				if(context != null) {
					context.getCounter(Counters.HOMOGENEOUS_COLUMNS).increment(1);
					context.getCounter(Counters.ATTRACTORS).increment(1);
				}
				
				rowInd[valPtr] = rowInd[cs];
				val[valPtr += incr] = val[cs];
				if(max_s < 1) max_s = 1;
				continue;
			default:
				break;
			}
			
			float sum = 0.0f;
			float max = 0.0f;
			float min = 1.0f;
			
			for(int i = cs; i < ct; i++){
				float pow = (float) Math.pow(val[i], inflation);
				val[i] = pow;
				sum += pow;
				if(max < pow)
					max = pow;
			}
			
			final float tresh = computeTreshold(sum/k, max);
			
			int selected = 0;
			sum = 0.0f;			
			
			for(int i = cs; i < ct; i++){
				if(val[i] >= tresh){
					selection[selected++] = i;
					sum += val[i];
				} else {
					if(context != null) context.getCounter(Counters.CUTOFF).increment(1);
				}
			}
			
			if(selected > this.selection){
				if(context != null) context.getCounter(Counters.PRUNE).increment(selected - this.selection);
				sum = selector.select(val, selection, selected, this.selection);
				selected = this.selection;
			}
			
			max /= sum;
			
			final int sel_s = top_aligned ? 0 : selected - 1;
			final int sel_t = top_aligned ? selected : - 1;
			
			for(int i  = sel_s; i != sel_t; i += incr){
				int idx = selection[selected];
				float result = val[idx] / sum;
				if(result > 0.5f)
					if(context != null) context.getCounter(Counters.ATTRACTORS).increment(1);
				if(min > result)
					min = result;
				rowInd[valPtr] = rowInd[idx];
				val[valPtr += incr] = val[idx];
			}
			
			if(min == max){
				if(context != null) context.getCounter(Counters.HOMOGENEOUS_COLUMNS).increment(1);
			}
			
			if(max_s < selected) max_s = selected;
			
		}
		
		colPtr[colPtr_t+prev_col_direction] = valPtr+prev_col_direction;
		if(context != null) context.getCounter(Counters.NNZ).increment(size());
		return max_s;
	}
	
	private final float computeTreshold(float avg, float max) {
		//TODO a,b
		float tresh = 0.9f*avg*(1-2.0f*(max-avg));
		tresh = tresh < cutoff ? cutoff : tresh;
		return tresh < max ? tresh : max;
	}
	
	@Override
	protected ReadOnlyIterator<CSCSlice> getSubBlockIterator(SliceId id) {
		return new SubBlockIterator(id);
	}

	@Override
	public int size() {
		return colPtr[nsub];
	}

	private final class SubBlockIterator extends ReadOnlyIterator<CSCSlice>{
		
		private final SliceId id;
		private final Queue<SubBlockSlice> queue = new PriorityQueue<CSCSlice.SubBlockSlice>(nsub);
		private final List<SubBlockSlice> list = new ArrayList<CSCSlice.SubBlockSlice>(nsub);
		private final int[] offset = new int[nsub];
		
		public SubBlockIterator(SliceId sliceId) {
			this.id = sliceId;
			
			if(view == null) {
				view = new SubBlockView();
			}
			
			System.arraycopy(colPtr, 0, offset, 0, nsub);			
			
			for(int column = 0; column < nsub; column++) {
				fetch(column);
			}
		}
		
		@Override
		public boolean hasNext() {
			return !queue.isEmpty();
		}
	
		@Override
		public CSCSlice next() {
			
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
			
			return CSCSlice.this;
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
			return id == o.id ? 0 : id < o.id ? -1 : 1;
		}		
	}
	
	private final class SubBlockView {
		long row_shift = 0;
		final int[] size = new int[nsub];
		final int[] offset = new int[nsub];
	}

	@Override
	public Iterable<MatrixEntry> dump() {
		return new Iterable<MatrixEntry>() {

			@Override
			public Iterator<io.writables.MCLMatrixSlice.MatrixEntry> iterator() {
				return new EntryIterator();
			}
			
		};
	}
	
	private final class EntryIterator extends ReadOnlyIterator<MatrixEntry> {

		private final MatrixEntry entry = new MatrixEntry();
		private final int l = colPtr[nsub];
		private int col_end = colPtr[1];
		private int col = 0;
		private int i = colPtr[0];
		
		@Override
		public boolean hasNext() {
			return i < l;
		}

		@Override
		public MatrixEntry next() {

			while(i == col_end) {
				col_end = colPtr[++col + 1];
			}
			
			entry.col = col;
			entry.row = rowInd[i];
			entry.val = val[i++];
			
			return entry;
		}
		
	}

}
