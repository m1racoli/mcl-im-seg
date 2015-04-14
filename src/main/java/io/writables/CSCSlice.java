/**
 * 
 */
package io.writables;

import io.heap.FibonacciHeap;
import iterators.ReadOnlyIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import mapred.MCLStats;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Cedrik Neumann
 *
 */
public final class CSCSlice extends FloatMatrixSlice<CSCSlice> {

	private static final Logger logger = LoggerFactory.getLogger(CSCSlice.class);
	
	private float[] val = null;
	private long[] rowInd = null;
	private int[] colPtr = null;
	
	private boolean top_aligned = true;
	private SubBlockView view = null;
	private SubBlockIterator subBlockIterator = null;

	public CSCSlice(){}
	
	public CSCSlice(Configuration conf){
		setConf(conf);
	}
	
	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		val = new float[max_nnz];
		rowInd = new long[max_nnz];
		colPtr = new int[nsub+1];
	}

	@Override
	public void clear(){
		Arrays.fill(colPtr, 0);
		top_aligned = true;
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
		
		assert colPtr[0] == 0;
		
		for(int i = colPtr[nsub] - 1; i >= 0; --i){
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
				final int size = view.size[col];
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
	public int fill(Iterable<SliceEntry> entries) {
		top_aligned = true;
		int current_col = 0;
		long last_row = -1;
		int l = 0;
		colPtr[0] = 0;
		int kmax = 0;
		int cs = 0;
		
		for(SliceEntry entry : entries){
			
			if(entry.col == current_col && entry.row == last_row){
				continue;
			}
			
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
		
		return kmax;
	}
	
	private static void arrayfill(int[] a, int fromIndex, int toIndex, int val) {
        for (int i = fromIndex; i < toIndex; i++)
            a[i] = val;
    }

	@Override
	public void add(CSCSlice m) {
		
		if(top_aligned) {
			
			if(isEmpty()){
				flip(this,m);
				return;
			}
			
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
	public CSCSlice multipliedBy(CSCSlice m) {

		assert top_aligned;
		
		final float[] tmp_val = new float[kmax];
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
				
				final float factor = tmp_val[i];
				final int target_col = (int) tmp_rowInd[i]; //this is gonna be normalized to column range (< n_sub)
				
				final int target_cs = m.colPtr[target_col];
				final int target_ct = m.colPtr[target_col + 1];
				
				if(target_cs == target_ct){continue;};
				
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

	private static void flip(CSCSlice m1, CSCSlice m2) {
		float[] val_tmp = m1.val;
		long[] rowInd_tmp = m1.rowInd;
		int[] colPtr = m1.colPtr;
		boolean top_aligned = m1.top_aligned;
		m1.val = m2.val;
		m1.rowInd = m2.rowInd;
		m1.colPtr = m2.colPtr;
		m1.top_aligned = m2.top_aligned;
		m2.val = val_tmp;
		m2.rowInd = rowInd_tmp;
		m2.colPtr = colPtr;
		m2.top_aligned = top_aligned;
	}
	
	private final int addForw(int s1, int t1, CSCSlice m, int s2, int t2, int pos) {
		int p = pos, i1 = s1, i2 = s2;
		
		while (i1 < t1 && i2 < t2) {
			final long r1 = rowInd[i1];
			final long r2 = m.rowInd[i2];
			
			if (r1 == r2) {
				rowInd[p] = r1;
				val[p++] = val[i1++] + m.val[i2++];				
			} else {
				if (r1 < r2) {
					rowInd[p] = r1;
					val[p++] = val[i1++];	
				} else {
					rowInd[p] = r2;
					val[p++] = m.val[i2++];
				}
			}
		}
		
		while (i1 < t1) {
			rowInd[p] = rowInd[i1];
			val[p++] = val[i1++];
		}

		while(i2 < t2) {
			rowInd[p] = m.rowInd[i2];
			val[p++] = m.val[i2++];
		}
		
		return p - pos;
	}
	
	private final int addMultForw(int s1, int t1, CSCSlice m, int s2, int t2, int pos, float factor) {
		
		int p = pos, i1 = s1, i2 = s2;
		
		while (i1 < t1 && i2 < t2) {
			final long r1 = rowInd[i1];
			final long r2 = m.rowInd[i2];
			
			if (r1 == r2) {
				rowInd[p] = r1;
				val[p++] = val[i1++] + factor * m.val[i2++];				
			} else {
				if (r1 < r2) {
					rowInd[p] = r1;
					val[p++] = val[i1++];	
				} else {
					rowInd[p] = r2;
					val[p++] = factor * m.val[i2++];
				}
			}
		}
		
		while (i1 < t1) {
			rowInd[p] = rowInd[i1];
			val[p++] = val[i1++];
		}

		while(i2 < t2) {
			rowInd[p] = m.rowInd[i2];
			val[p++] = factor * m.val[i2++];
		}
		
		return p - pos;
	}
	
	private final int addBack(int s1, int t1, CSCSlice m, int s2, int t2, int pos) {
		int p = pos, i1 = t1 - 1, i2 = t2 - 1 ;
		
		while (i1 >= s1 && i2 >= s2) {
			long r1 = rowInd[i1];
			long r2 = m.rowInd[i2];
			
			if (r1 == r2) {
				rowInd[--p] = r1;
				val[p] = val[i1--] + m.val[i2--];
			} else {
				if (r1 > r2) {
					rowInd[--p] = r1;
					val[p] = val[i1--];
				} else {
					rowInd[--p] = r2;
					val[p] = m.val[i2--];
				}
			}
		}
			
		while (i1 >= s1) {
			rowInd[--p] = rowInd[i1];
			val[p] = val[i1--];
		}			
		
		while (i2 >= s2) {
			rowInd[--p] = m.rowInd[i2];
			val[p] = m.val[i2--];	
		}
		
		return pos - p;
	}
	
	private final int addMultBack(int s1, int t1, CSCSlice m, int s2, int t2, int pos, float factor) {
		
		int p = pos, i1 = t1 - 1, i2 = t2 - 1 ;
		
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
	public void inflateAndPrune(MCLStats stats) {
		
		final int[] selection = new int[kmax];
		int valPtr = 0;
		float threshold;
		
		for(int col_start = 0, col_end = 1, end = nsub; col_start < end; col_start = col_end++) {

			int cs = colPtr[col_start];
			int ct = colPtr[col_end];
			int v_n = ct-cs;
			
			colPtr[col_start] = valPtr;
			
			switch(v_n){
			case 0:
				continue;
			case 1:				
				rowInd[valPtr] = rowInd[cs];
				val[valPtr++] = 1.0f;
				if(stats.kmax < 1) stats.kmax = 1;
				stats.attractors++;
				stats.homogen++;
				continue;
			default:
				break;
			}
			
			if(auto_prune){
				double s = 0.0;
				double m = 0.0;
				
				for(int i = ct-1; i >= cs; --i) {
					float v = (float) Math.pow(val[i], inflation);
					val[i] = v;
					if(m < v) m = v;
					s += v;
				}
				
				threshold = computeTreshold(s/v_n, m);
			} else {
				threshold = cutoff;
			}
			
			v_n = threshPrune(val, cs, ct, selection, stats, threshold);
			
			if(v_n > select){
				stats.prune += v_n - select;
				selectionPrune(val, selection, v_n, select);
				v_n = select;
			}
			
			if(v_n == 1){
				rowInd[valPtr] = rowInd[selection[0]];
				val[valPtr++] = 1.0f;
				stats.homogen++;
				stats.attractors++;
				continue;
			}
			
			for(int i  = 0; i < v_n; i++){
				final int idx = selection[i];
				rowInd[valPtr] = rowInd[idx];
				val[valPtr++] = val[idx];
			}
			
			ct = valPtr;
			cs = ct - v_n;
			
			double max = 0.0;
			double center = 0.0;
			double s = 0.0;
			
			if(auto_prune){
				for(int i = cs; i < ct; i++){
					s += val[i];
				}
			} else {
				for(int i = cs; i < ct; i++){
					float v = (float) Math.pow(val[i], inflation);
					val[i] = v;
					s += v;
				}
			}
			
			float sf = (float) s;
			
			for(int i = cs; i < ct; i++){
				float v = val[i] / sf;
				val[i] = v;
				if(max < v) max = v;
				center += v*v;
			}
			
			double chaos = (max - center) * (ct-cs);
			if(max > 0.5) stats.attractors++;
			if(chaos < 1.0e-4) stats.homogen++;
			if(stats.chaos < chaos) stats.chaos = chaos;
			if(stats.kmax < v_n) stats.kmax = v_n;
		}
		
		colPtr[nsub] = valPtr;
	}
	
	@Override
	public void makeStochastic() {
		for(int col_start = 0, col_end = 1, end = nsub; col_start < end; col_start = col_end++) {
			normalize(val, colPtr[col_start], colPtr[col_end]);
		}
	}
	
	@Override
	protected ReadOnlyIterator<CSCSlice> getSubBlockIterator(SliceId id) {
		
		assert top_aligned;
		
		if(subBlockIterator == null){
			subBlockIterator = new SubBlockIterator();
			view = new SubBlockView();
		}
		subBlockIterator.reset(id);
		return subBlockIterator;
	}

	@Override
	public int size() {
		
		if(view != null){
			int s = 0;
			for(int i = nsub; i>0;){
				s += view.size[--i];
			}
			return s;
		}
		
		return colPtr[nsub] - colPtr[0];
	}

	private final class SubBlockIterator extends ReadOnlyIterator<CSCSlice>{
		private final int nsub = CSCSlice.this.nsub;
		private SliceId id = null;
		private final Queue<SubBlockSlice> queue = javaQueue ?
						new PriorityQueue<CSCSlice.SubBlockSlice>(nsub):
						new FibonacciHeap<CSCSlice.SubBlockSlice>(nsub);
		private final List<SubBlockSlice> list = new ArrayList<CSCSlice.SubBlockSlice>(nsub);
		private final int[] offset = new int[nsub];
		
		private void reset(SliceId id){
			this.id = id;
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
		public CSCSlice next() {
			
			final SubBlockSlice first = queue.poll();
			list.add(first);
			
			SubBlockSlice current = queue.peek();
			while(current != null && first.id == current.id) {
				list.add(queue.poll());
				current = queue.peek();
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
			
			while(i != t && rowInd[i] < end){
				i++;
			}
			
			queue.offer(new SubBlockSlice(column, i - s, id));
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
		private final int t = colPtr[nsub];
		private int col_end = colPtr[1];
		private int col = 1;
		private int i = colPtr[0];
		
		@Override
		public boolean hasNext() {
			return i < t;
		}

		@Override
		public SliceEntry next() {

			while(i == col_end) {
				entry.col = col;
				col_end = colPtr[++col];
			}
			
			entry.row = rowInd[i];
			entry.val = val[i++];
			
			return entry;
		}
		
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof CSCSlice){
			CSCSlice o = (CSCSlice) obj;

			if (colPtr.length != o.colPtr.length) {
				return false;
			}
			
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
	public void addLoops(SliceIndex id) {
		
		final long shift = nsub * (long) id.getSliceId();
		
		for(int col = 0, end = nsub; col < end; ++col){
			final int cs = colPtr[col];
			final int ct = colPtr[col + 1];
			
			if(ct - cs == 0){
				continue;
			}
			
			final long colInd = shift + (long) col;
			
			float max = 0.0f;
			int diag = -1;
			
			for(int i = cs; i < ct; i++){
				if(rowInd[i] == colInd){
					diag = i;
				} else {
					if(max < val[i]) max = val[i];
				}
			}
			
			if(diag == -1){
				//cannot insert additional element
				logger.error("diagonal element does not exist");
				throw new IllegalStateException("diagonal element does not exist. id="+id.getSliceId()+" col: "+col);
			}
			
			if(max == 0.0f) max = 1.0f;
			
			val[diag] = max;
		}
	}

	@Override
	public double sumSquaredDifferences(CSCSlice o) {
		
		if(colPtr.length != o.colPtr.length){
			throw new RuntimeException(String.format("slices are incompatible in nsub: %d != %d",colPtr.length,o.colPtr.length));
		}
		
		double sum = 0.0;
		
		for(int col = nsub; col > 0;){
			
			int t1 = colPtr[col], t2 = o.colPtr[col], i1 = colPtr[--col], i2 = o.colPtr[col];
			
			while(i1 < t1 && i2 < t2){
				long r1 = rowInd[i1];
				long r2 = o.rowInd[i2];
				
				if (r1 == r2) {
					sum += (val[i1] - o.val[i2]) * (val[i1] - o.val[i2]);
					i1++; i2++;
				} else {
					if (r1 < r2) {
						sum += val[i1] * val[i1]; i1++;
					} else {
						sum += o.val[i2] * o.val[i2]; i2++;
					}
				}
			}
			
			while(i1 < t1){
				sum += val[i1] * val[i1]; i1++;
			}
			
			while(i2 < t2){
				sum += o.val[i2] * o.val[i2]; i2++;
			}
		
		}
		
		return sum;
	}

}
