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

import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author Cedrik
 *
 */
public final class NativeCSCSlice extends MCLMatrixSlice<NativeCSCSlice> {

	private ByteBuffer bb = null;
	
	/**
	 * 
	 */
	public NativeCSCSlice() {
		// TODO Auto-generated constructor stub
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
		// TODO Auto-generated method stub
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#clear()
	 */
	@Override
	public void clear() {
		NativeCSCSliceHelper.clear(bb,nsub);
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
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#multipliedBy(io.writables.MCLMatrixSlice, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public NativeCSCSlice multipliedBy(NativeCSCSlice m,
			TaskAttemptContext context) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#getSubBlockIterator(io.writables.SliceId)
	 */
	@Override
	protected ReadOnlyIterator<NativeCSCSlice> getSubBlockIterator(SliceId id) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#size()
	 */
	@Override
	public int size() {
		return bb.getInt(nsub)-bb.getInt(0);
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#inflateAndPrune(mapred.MCLStats, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public void inflateAndPrune(MCLStats stats, TaskAttemptContext context) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#makeStochastic(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public void makeStochastic(TaskAttemptContext context) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#addLoops(io.writables.SliceIndex)
	 */
	@Override
	public void addLoops(SliceIndex id) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#sumSquaredDifferences(io.writables.MCLMatrixSlice)
	 */
	@Override
	public double sumSquaredDifferences(NativeCSCSlice other) {
		// TODO Auto-generated method stub
		return 0;
	}

}
