/**
 * 
 */
package io.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import mapred.Counters;
import mapred.Selector;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import util.ReadOnlyIterator;

/**
 * @author Cedrik
 *
 */
public final class CSCSlice extends MCLMatrixSlice<CSCSlice> {

	private int l = 0;
	private final float[] val;
	private final long[] rowInd;
	private final int[] colPtr;
	
	private boolean left_to_right = true;
	private transient Selector selector = null;
	
	public CSCSlice() {this(getInitNnz());}

	public CSCSlice(int init_nnz){
		super(init_nnz);
		val = new float[init_nnz];
		rowInd = new long[init_nnz];
		colPtr = new int[n_sub+1];
	}

	@Override
	public void clear(){
		l = 0;
		Arrays.fill(colPtr, 0);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		left_to_right = true;
		l = readInt(in);

		for(int i = 0; i < l; i++){
			val[i] = in.readFloat();
			rowInd[i] = readLong(in);
		}
		for(int i = 0; i < colPtr.length;i++){
			colPtr[i] = readInt(in);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		writeInt(out, l);
		
		final int s = left_to_right ? 0 : val.length - l;
		final int t = left_to_right ? l : val.length;
		
		for(int i = s; i < t; i++){
			out.writeFloat(val[i]);
			writeLong(out, rowInd[i]);
		}
		for(int i = 0; i < colPtr.length; i++){
			writeInt(out, colPtr[i]);
		}		
	}
	
	@Override
	public String toString() {		
		if(l == 0) return "Column[empty]";
		
		return String.format("Column[nnz: %d]", l);
	}
	
	@Override
	public void add(IntWritable col, LongWritable row, Iterable<Float> values) {
		left_to_right = true;
		int current_col = 0;
		
		for(Float value : values){
			final int value_column = col.get();
			Arrays.fill(colPtr, current_col+1, value_column, l);
			current_col = value_column;
			
			val[l] = value;
			rowInd[l++] = row.get();
		}
		
		Arrays.fill(colPtr, current_col+1, colPtr.length, l);
	}

	@Override
	public void add(CSCSlice vec) {
		
		//TODO
	}

	@Override
	public CSCSlice getProduct(CSCSlice subBlock) {
		//TODO
		subBlock.left_to_right = false;
		return subBlock;
	}

	@Override
	public void inflateAndPrune(TaskInputOutputContext<?, ?, ?, ?> context) {
		
		left_to_right = !left_to_right;
		
		if(selector == null){
			selector = new Selector(); //TODO custom class
		}
		//TODO private final
		final int S = getS();
		final float I = getI();
		
		final int[] selection = new int[k_max];
		final int incr = left_to_right ? 1 : -1;		
		final int colPtr_s = left_to_right ? 0 : colPtr.length - 1;
		final int colPtr_t = left_to_right ? colPtr.length - 1 : 0;
		int valPtr = left_to_right ? 0 : val.length - 1;		
		final int prev_col_direction = left_to_right ? 0 : 1;
		
		for(int currenct_col = colPtr_s; currenct_col < colPtr_t; currenct_col += incr) {
			
			final int cs = colPtr[currenct_col];
			final int ct = colPtr[currenct_col+1];
			final int k = ct-cs;
			
			colPtr[currenct_col+prev_col_direction] = valPtr; //TODO check correctness
			
			switch(k){
			case 0:
				context.getCounter(Counters.EMPTY_COLUMNS).increment(1);
				continue;
			case 1:
				if(val[cs] != 1.0f){
					//TODO remove for non debug
					context.getCounter(Counters.SINGLE_COLUMN_NOT_ONE).increment(1);
				}
				context.getCounter(Counters.HOMOGENEOUS_COLUMNS).increment(1);
				context.getCounter(Counters.ATTRACTORS).increment(1);
				rowInd[valPtr] = rowInd[cs];
				val[valPtr += incr] = val[cs];
				continue;
			default:
				break;
			}
			
			float sum = 0.0f;
			float max = 0.0f;
			float min = 1.0f;
			
			for(int i = cs; i < ct; i++){
				float pow = (float) Math.pow(val[i], I);
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
					context.getCounter(Counters.CUTOFF).increment(1);
				}
			}
			
			if(selected > S){
				context.getCounter(Counters.PRUNE).increment(selected - S);
				sum = selector.select(val, selection, selected, S);
				selected = S;
			}
			
			max /= sum;
			
			for(int i  = 0; i < selected; i++){
				int idx = selection[selected];
				float result = val[idx] / sum;
				if(result > 0.5f)
					context.getCounter(Counters.ATTRACTORS).increment(1);
				if(min > result)
					min = result;
				rowInd[valPtr] = rowInd[idx];
				val[valPtr += incr] = val[idx];
			}
			
			if(min == max){
				context.getCounter(Counters.HOMOGENEOUS_COLUMNS).increment(1);
			}
			
		}
		
		//TODO last/first column = valPtr
		
		context.getCounter(Counters.NNZ).increment(size());
	}
	
	private final float computeTreshold(float avg, float max) {
		//TODO a,b,p
		float tresh = 0.9f*avg*(1-2.0f*(max-avg));
		tresh = tresh < 1.0e-4f ? 1.0e-7f : tresh;
		return tresh < max ? tresh : max;
	}
	
	@Override
	protected ReadOnlyIterator<CSCSlice> getSubBlockIterator(SliceId id) {
		return new SubBlockIterator(id);
	}

	@Override
	public int size() {
		return l;
	}

	private final class SubBlockIterator extends ReadOnlyIterator<CSCSlice>{
		//TODO all!
		private final FloatWritable value = new FloatWritable();
		private final SliceId id;
		private int i = 0;
		
		private SubBlockIterator(SliceId id) {
			this.id = id;
		}
		
		@Override
		public boolean hasNext() {
			return i < l;
		}
	
		@Override
		public CSCSlice next() {
			//TODO
			return null;
		}
		
	}

}
