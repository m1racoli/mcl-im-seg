/**
 * 
 */
package io.writables;

import iterators.ReadOnlyIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeSet;

import mapred.MCLStats;

import org.apache.commons.math3.linear.DefaultRealMatrixPreservingVisitor;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Cedrik
 *
 */
public class OpenMapSlice extends DoubleMatrixSlice<OpenMapSlice> {

	private static final Logger logger = LoggerFactory.getLogger(OpenMapSlice.class);
	
	private OpenMapRealMatrix matrix;
	
	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		
		int int_n = (int) n;
		
		assert n == int_n : "class only supports dimensions < INT.MAX";
		
		matrix = new OpenMapRealMatrix((int) n, nsub);
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		
		WritableUtils.writeVInt(out, matrix.getRowDimension());
		
		matrix.walkInOptimizedOrder(new DefaultRealMatrixPreservingVisitor() {
			
			@Override
			public void visit(int row, int column, double value) {
				if(value == 0.0){
					return;
				}
				
				try {
					WritableUtils.writeVInt(out, row);
					WritableUtils.writeVInt(out, column);
					out.writeDouble(value);					
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				
			}
			
		});
		
		WritableUtils.writeVInt(out, -1);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int row_dim = WritableUtils.readVInt(in);
		
		matrix = matrix.createMatrix(row_dim, nsub);
		
		int row = WritableUtils.readVInt(in);
		while(row != -1){
			matrix.setEntry(row, WritableUtils.readVInt(in), in.readDouble());
			row = WritableUtils.readVInt(in);
		}
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#clear()
	 */
	@Override
	public void clear() {
		matrix = matrix.createMatrix((int) n, nsub);
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#fill(java.lang.Iterable)
	 */
	@Override
	public int fill(Iterable<io.writables.SliceEntry> entries) {
		int[] k = new int[nsub];
		
		matrix = matrix.createMatrix((int) n, nsub);
		for(SliceEntry e : entries){
			matrix.setEntry((int) e.row, e.col, e.val);
			k[e.col]++;
		}
		
		int kmax = 0;
		for(int i = 0; i < nsub; i++){
			kmax = Math.max(kmax, k[i]);
		}
		
		return kmax;
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#dump()
	 */
	@Override
	public Iterable<io.writables.SliceEntry> dump() {

		final TreeSet<SliceEntry> entries = new TreeSet<SliceEntry>();
		
		matrix.walkInOptimizedOrder(new DefaultRealMatrixPreservingVisitor(){
			@Override
			public void visit(int row, int column, double value) {
				if(value == 0.0){
					return;
				}
				
				SliceEntry e = new SliceEntry();
				e.col = column;
				e.row = row;
				e.val = (float) value;
				entries.add(e);
			}
		});
				
		return entries;
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#add(io.writables.MCLMatrixSlice)
	 */
	@Override
	public void add(OpenMapSlice m) {
		matrix = matrix.add(m.matrix);
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#multipliedBy(io.writables.MCLMatrixSlice, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public OpenMapSlice multipliedBy(OpenMapSlice m) {		
		OpenMapSlice o = getInstance();
		o.matrix = m.matrix.multiply(matrix);
		return o;
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#getSubBlockIterator(io.writables.SliceId)
	 */
	@Override
	protected ReadOnlyIterator<OpenMapSlice> getSubBlockIterator(final SliceId id) {
		
		return new ReadOnlyIterator<OpenMapSlice>() {

			final OpenMapSlice block = getInstance();
			final int slices = (int) (n/nsub);
			int i = 0;
			
			@Override
			public boolean hasNext() {
				return i < slices;
			}

			@Override
			public OpenMapSlice next() {
				id.set(i);
				
				int cs = nsub * i++;
				int ct = cs + nsub - 1;
				
				block.matrix = new OpenMapRealMatrix(nsub, nsub);
				block.matrix.setSubMatrix(matrix.getSubMatrix(cs, ct, 0, nsub-1).getData(),0,0);
				return block;
			}
		};
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#size()
	 */
	@Override
	public int size() {
		int cnt = 0;
		double[][] data = matrix.getData();
		
		for(int i = 0; i < data.length;i++){
			for(int j = 0; j<data[i].length;j++){
				if(data[i][j] > 0.0){
					cnt++;
				}
			}
		}
		
		return cnt;
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#inflateAndPrune(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public void inflateAndPrune(MCLStats stats, TaskAttemptContext context) {
		int kmax = 0;
		int[] selection = new int[kmax];
		for(int col = 0, end = nsub; col < end; col++){
			final double[] val = matrix.getColumn(col);
			inflate(val, 0, val.length);
			int selected = prune(val, 0, val.length, selection, context);
			kmax = Math.max(kmax, selected);
			final double[] newval = new double[val.length];
			for(int i = 0; i < selected; i++) {
				final int idx = selection[i];
				newval[idx] = val[idx];
			}
			normalize(newval, 0, newval.length, stats);
			matrix.setColumn(col, newval);
		}
		//TODO
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof OpenMapSlice){
			OpenMapSlice o = (OpenMapSlice) obj;
			return matrix.equals(o.matrix);
		}
		return false;
	}

	@Override
	public void makeStochastic(MCLStats stats) {
		
		for(int col = 0, end = matrix.getColumnDimension(); col < end; col++){
			final double[] val = matrix.getColumn(col);
			normalize(val, 0, val.length, stats);
			matrix.setColumn(col, val);
		}

	}

	@Override
	public void addLoops(SliceIndex id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public double sumSquaredDifferences(OpenMapSlice other) {
		// TODO Auto-generated method stub
		return 0;
	}

}
