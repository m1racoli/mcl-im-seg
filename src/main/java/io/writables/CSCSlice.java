/**
 * 
 */
package io.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Queue;

import mapred.Counters;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import util.ReadOnlyIterator;

/**
 * @author Cedrik
 *
 */
public final class CSCSlice extends MCLMatrixSlice {

	private long[] rows;
	private float[] vals;
	private Float a = null;
	private int l = 0;
	private Queue<Item> buffer = null;
	
	public CSCSlice() {super();}

	public CSCSlice(int init_nnz){
		super(init_nnz);
		rows = new long[init_nnz];
		vals = new float[init_nnz];
	}

	@Override
	public void clear(){
		l = 0;
		a = null;
	}

	@Override
	protected void add(MCLMatrixSlice m) {
		
		CSCSlice vec = (CSCSlice) m;
		
		long[] new_rows = new long[init_nnz];
		float[] new_vals = new float[init_nnz]; //TODO
		
		int new_l = 0;
		for(int i = 0, j = 0; i < l || j < vec.l;){
			
			if(i == l){
				new_vals[new_l] = vec.vals[j];
				new_rows[new_l++] = vec.rows[j++];
				continue;
			}
			
			if(j == vec.l){
				new_vals[new_l] = vals[i];
				new_rows[new_l++] = rows[i++];
				continue;
			}
			
			if(rows[i] == vec.rows[j]){
				new_vals[new_l] = vals[i] +vec.vals[j];
				new_rows[new_l++] = rows[i];
				i++;
				j++;
			} else {
				if(rows[i] < vec.rows[j]){
					new_vals[new_l] = vals[i];
					new_rows[new_l++] = rows[i++];
				} else {
					new_vals[new_l] = vec.vals[j];
					new_rows[new_l++] = vec.rows[j++];
				}
			}
		}

		rows = new_rows;
		vals = new_vals;
		l = new_l;
	}
	
	@Deprecated
	private final void ensureSize(int len, boolean keepData){
		final int size = rows.length;
		
		if(size >= len){
			return;
		}
		
		long[] row_tmp = new long[len];
		float[] val_tmp = new float[len];
		if(keepData){
			System.arraycopy(rows, 0, row_tmp, 0, l);
			System.arraycopy(vals, 0, val_tmp, 0, l);
		}
		rows = row_tmp;
		vals = val_tmp;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		final int newLen = WritableUtils.readVInt(in);
		ensureSize(newLen, false);
		for(int i = 0; i < newLen; i++){
			rows[i] = WritableUtils.readVLong(in);
			vals[i] = in.readFloat();
		}
		l = newLen;
		a = null;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		
		WritableUtils.writeVInt(out, l);
		
		if(a == null){
			for(int i = 0; i < l; i++){
				WritableUtils.writeVLong(out, rows[i]);
				out.writeFloat(vals[i]);
			}
		} else {
			final float a = this.a;
			for(int i = 0; i < l; i++){
				WritableUtils.writeVLong(out, rows[i]);
				out.writeFloat(vals[i]*a);
			}
		}
	}
	
	@Override
	public String toString() {
		
		if(l == 0){
			return "Column[empty]";
		}
		
		StringBuilder builder = new StringBuilder("Column[");
		builder.append(String.format("%d:%4.3f", rows[0], vals[0]));
		for(int i = 1; i < l;i++){
			builder.append(String.format(", %d:%4.3f", rows[i], vals[i]));
		}
		builder.append("]");
		return builder.toString();
	}
	
	@Override
	protected MCLMatrixSlice product(MCLMatrixSlice subBlock) {
		CSCSlice c = (CSCSlice) subBlock;
		a = c.vals[0];
		return this;
	}

	@Override
	public void process(TaskInputOutputContext<?, ?, ?, ?> context) {
		
		if(buffer == null){
			buffer = new PriorityQueue<CSCSlice.Item>(getKMax());
		}
		
		final float p = getP();
		final int P = getPInv();
		final double I = getI();
		
		double sum = 0.0;
		float max = 0;
		float min = Float.POSITIVE_INFINITY;
		
		//TODO better pruning
		
		for(int i = 0; i < l; i++){
			if(vals[i] < p){
				context.getCounter(Counters.CUTOFF).increment(1);
				continue;
			}
			
			buffer.add(Item.get(rows[i], (float) Math.pow(vals[i],I)));
		}
		
		final int min_l = Math.min(P, buffer.size());
		if(P < buffer.size()){
			context.getCounter(Counters.PRUNE).increment(buffer.size() - P);
		}
		l = min_l;
		
		for(int i = 0; i < min_l; i++){
			Item item = buffer.remove();
			rows[i] = item.idx;
			vals[i] = item.val;
			sum += item.val;
			min = Math.min(min,item.val);
			max = Math.max(max, item.val);
		}
		
		buffer.clear();
		
		if(min == max){
			context.getCounter(Counters.CONVERGED_COLUMNS).increment(1);
		}
		
		a = (float) (1.0/sum);
		
		context.getCounter(Counters.NON_NULL_VALUES).increment(size());
	}

	@Override
	protected ReadOnlyIterator<MCLMatrixSlice> getSubBlockIterator(LongWritable id) {
		return new SubBlockIterator(id);
	}

	@Override
	public int size() {
		return l;
	}

	private final class SubBlockIterator extends ReadOnlyIterator<MCLMatrixSlice>{
	
		private final FloatWritable val = new FloatWritable();
		private final LongWritable id;
		private int i = 0;
		
		private SubBlockIterator(LongWritable id) {
			this.id = id;
		}
		
		@Override
		public boolean hasNext() {
			return i < l;
		}
	
		@Override
		public MCLMatrixSlice next() {
			id.set(rows[i]);
			val.set(vals[i++]);
			return val;
		}
		
	}

	public static final class Item implements Comparable<Item>{
		public final long idx;
		public final float val;
		
		private Item(long idx, float val){
			this.idx = idx; this.val = val;
		}
		
		static Item get(long idx, float val){
			return new Item(idx, val);
		}
		
		@Override
		public int compareTo(Item o) {
			return -Float.compare(val, o.val);
		}
	}

	@Override
	protected void add(LongWritable row, MCLMatrixSlice subBlock) {
		rows[l] = row.get();
		vals[l++] = subBlock.get();
	}

	@Override
	public void contruct(LongWritable row, Iterable<Float> values) {
		
		float sum = 0.0f;
		for(Float val : values){
			rows[l] = row.get();
			vals[l++] = val;
			sum += val;
		}
		
		a = 1.0f/sum;
	}

}
