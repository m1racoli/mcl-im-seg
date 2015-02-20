/**
 * 
 */
package io.writables.nat;

import io.writables.MCLMatrixSlice;
import io.writables.SliceEntry;
import io.writables.SliceId;
import io.writables.SliceIndex;
import iterators.ReadOnlyIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import mapred.MCLStats;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Cedrik
 *
 */
public final class NativeCSCSlice extends MCLMatrixSlice<NativeCSCSlice> {

	private static final byte TOP_ALIGNED = 0x00;
	
	private ByteBuffer bb = null;
	
	/**
	 * 
	 */
	public NativeCSCSlice() {}
	
	public NativeCSCSlice(Configuration conf){
		setConf(conf);
	}
	
	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		NativeCSCSliceHelper.setNsub(nsub);
		NativeCSCSliceHelper.setSelect(select);
		NativeCSCSliceHelper.setAutoprune(auto_prune);
		NativeCSCSliceHelper.setInflation(inflation);
		NativeCSCSliceHelper.setCutoff(cutoff);
		NativeCSCSliceHelper.setPruneA(pruneA);
		NativeCSCSliceHelper.setPruneB(pruneB);
		NativeCSCSliceHelper.setMaxNnz(max_nnz);
		
		bb = ByteBuffer.allocateDirect((nsub + 1) * Integer.SIZE + max_nnz * (Long.SIZE + Float.SIZE) + 1);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		bb.put(0, TOP_ALIGNED);
		// TODO Auto-generated method stub
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#clear()
	 */
	@Override
	public void clear() {
		NativeCSCSliceHelper.clear(bb);
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#fill(java.lang.Iterable)
	 */
	@Override
	public int fill(Iterable<SliceEntry> entries) {
		// TODO Auto-generated method stub
		return 0;
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#dump()
	 */
	@Override
	public Iterable<SliceEntry> dump() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#add(io.writables.MCLMatrixSlice)
	 */
	@Override
	public void add(NativeCSCSlice m) {
		if(!NativeCSCSliceHelper.add(bb, m.bb)){
			ByteBuffer tmp = bb;
			bb = m.bb;
			m.bb = tmp;
		}
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#multipliedBy(io.writables.MCLMatrixSlice, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public NativeCSCSlice multipliedBy(NativeCSCSlice m) {
		NativeCSCSliceHelper.multiply(m.bb, bb);
		return this;
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#getSubBlockIterator(io.writables.SliceId)
	 */
	@Override
	protected ReadOnlyIterator<NativeCSCSlice> getSubBlockIterator(SliceId id) {
		return new SubBlockIterator();
	}
	
	private final class SubBlockIterator extends ReadOnlyIterator<NativeCSCSlice> {

		private final NativeCSCSlice b = new NativeCSCSlice();
		private ByteBuffer next = NativeCSCSliceHelper.startIterateBlocks(bb);
		
		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public NativeCSCSlice next() {
			b.bb = next;
			next = NativeCSCSliceHelper.nextBlock();
			return b;
		}
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#size()
	 */
	@Override
	public int size() {
		return bb.getInt(nsub*Integer.SIZE + 1) - bb.getInt(1);
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#inflateAndPrune(mapred.MCLStats, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public void inflateAndPrune(MCLStats stats) {
		NativeCSCSliceHelper.inflateAndPrune(bb, stats);
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#makeStochastic(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public void makeStochastic() {
		NativeCSCSliceHelper.makeStochastic(bb);
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof NativeCSCSlice){
			NativeCSCSlice o = (NativeCSCSlice) obj;
			return nsub == o.nsub && NativeCSCSliceHelper.equals(bb, o.bb);
		}
		return false;
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#addLoops(io.writables.SliceIndex)
	 */
	@Override
	public void addLoops(SliceIndex id) {
		NativeCSCSliceHelper.addLoops(bb, id.getSliceId());
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#sumSquaredDifferences(io.writables.MCLMatrixSlice)
	 */
	@Override
	public double sumSquaredDifferences(NativeCSCSlice other) {
		return NativeCSCSliceHelper.sumSquaredDifferences(bb, other.bb);
	}
	
	@Override
	public NativeCSCSlice deepCopy() {
		NativeCSCSlice other = getInstance();
		bb.rewind();
		other.bb.put(bb);
		bb.rewind();
		other.bb.flip();
		return other;
	}

}
