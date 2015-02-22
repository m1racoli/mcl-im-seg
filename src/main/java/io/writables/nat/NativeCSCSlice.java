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
import java.nio.ByteOrder;
import java.util.Iterator;

import mapred.MCLStats;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Cedrik
 *
 */
public final class NativeCSCSlice extends MCLMatrixSlice<NativeCSCSlice> {

	private static final byte TOP_ALIGNED = 0x00;
	private static final int INT_BYTES = Integer.SIZE/8;
	private static final int LONG_BYTES = Long.SIZE/8;
	private static final int ITEM_BYTES = LONG_BYTES + Float.SIZE/8;
	private static final int BUF_SIZE = 1024*1024;
	
	private final byte[] buf = new byte[BUF_SIZE];
	private ByteBuffer bb = null;
	private int COLPTR_LAST;
	private int HEADER_BYTES;
	
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
		NativeCSCSliceHelper.setParams(nsub,select,auto_prune,inflation,cutoff,pruneA,pruneB,kmax);
		
		COLPTR_LAST = nsub * INT_BYTES + 1;
		HEADER_BYTES = INT_BYTES * (nsub + 1) + 1;
		
		bb = ByteBuffer.allocateDirect(HEADER_BYTES + max_nnz * ITEM_BYTES);
		bb.order(ByteOrder.nativeOrder());
		clear();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		write(out, 0, HEADER_BYTES);
		write(out, itemBytesStart(), itemBytesLength());
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		read(in, 0, HEADER_BYTES);
		read(in, itemBytesStart(), itemBytesLength());
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
		int current_col = 0;
		long last_row = -1;
		int l = 0;
		int kmax = 0;
		int cs = 0;
		colPtr(0, 0);
		
		bb.put(0, TOP_ALIGNED);
		bb.position(HEADER_BYTES);
		
		for(SliceEntry e : entries){
			
			if(e.col == current_col && e.row == last_row){
				continue;
			}
			
			if(l == max_nnz) {
				throw new IllegalArgumentException(String.format("matrix already full with max nnz = %d. Please specify proper k_max value",max_nnz));
			}
			
			if(current_col > e.col) {
				throw new IllegalArgumentException(String.format("wrong column order: %d > %d",current_col,e.col));
			}
			
			if(current_col < e.col){				
				fillColPtr(current_col, e.col, l);
				kmax = Math.max(kmax, l-cs);
				last_row = -1;
				current_col = e.col;
				cs = colPtr(current_col);
			}
			
			if(last_row >= e.row) {
				throw new IllegalArgumentException(String.format("wrong row order in column %d: %d >= %d",current_col,last_row,e.row));
			}
			
			putItem(e.row, e.val);
			last_row = e.row;
		}
		
		fillColPtr(current_col, nsub, l);		
		kmax = Math.max(kmax, l-cs);
		
		return kmax;
	}
	
	private final void fillColPtr(int s, int t, int val){
		for(int i = t * INT_BYTES + 1, end = s * INT_BYTES + 1; i > end; i -= INT_BYTES){
			bb.putInt(i, val);
		}
	}
	
	private final void colPtr(int pos, int val){
		bb.putInt(pos * INT_BYTES + 1, val);
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#dump()
	 */
	@Override
	public Iterable<SliceEntry> dump() {
		return new Iterable<SliceEntry>() {
			
			@Override
			public Iterator<SliceEntry> iterator() {
				return new ItemIterator();
			}
		};
	}
	
	private final class ItemIterator extends ReadOnlyIterator<SliceEntry> {
		private final SliceEntry e = new SliceEntry();
		private int col = 1;
		private int i = colPtr(0);
		private int col_end = colPtr(1);
		private final int t = colPtr(nsub);
		
		@Override
		public boolean hasNext() {
			return i < t;
		}

		@Override
		public SliceEntry next() {

			while(i == col_end) {
				e.col = col;
				col_end = colPtr(++col);
			}
			
			e.row = rowInd(i);
			e.val = val(i++);
			
			return e;
		}
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
		private boolean ready = true;
		
		public SubBlockIterator() {
			b.bb = NativeCSCSliceHelper.startIterateBlocks(bb);
		}
		
		@Override
		public boolean hasNext() {
			check();
			return b.bb != null;
		}

		@Override
		public NativeCSCSlice next() {
			check(); 
			ready = false;
			return b;
		}
		
		private final void check() {
			if(!ready){
				b.bb = NativeCSCSliceHelper.nextBlock();
				ready = true;
			}
		}
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
		other.bb.limit(other.bb.capacity());
		
		//header
		bb.position(0);
		bb.limit(HEADER_BYTES);
		other.bb.position(0);		
		other.bb.put(bb);
		
		//items
		final int s = itemBytesStart();
		final int t = itemBytesEnd();
		bb.position(s);
		bb.limit(t);
		other.bb.position(s);
		other.bb.put(bb);
		
		return other;
	}
	
	private final void write(DataOutput out, int off, int len) throws IOException {
//		if(bb.hasArray()){
//			out.write(bb.array(), off, len);
//			return;
//		}
		
		bb.position(off);
		
		while(true)
		{
			if(len <= BUF_SIZE){
				bb.get(buf, 0, len);
				out.write(buf, 0, len);
				return;
			}
			
			len -= BUF_SIZE;
			bb.get(buf);
			out.write(buf);
		}
	}
	
	private final void read(DataInput in, int off, int len) throws IOException {
//		if(bb.hasArray()){
//			in.readFully(bb.array(), off, len);
//			return;
//		}
		
		bb.position(off);
		
		while(true)
		{
			if(len <= BUF_SIZE){
				in.readFully(buf, 0, len);
				bb.put(buf, 0, len);
				return;
			}
			
			len -= BUF_SIZE;
			in.readFully(buf);
			bb.put(buf);
		}
	}
	
	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#size()
	 */
	@Override
	public int size() {
		return itemsEnd() - itemsStart();
	}

	/**
	 * @return first position of items within item array
	 */
	private final int itemsStart() {
		return bb.getInt(1);
	}
	
	/**
	 * @return postion of items' end (exclusive) within item array
	 */
	private final int itemsEnd() {
		return bb.getInt(COLPTR_LAST);
	}
	
	private final int itemPosToAbsolute(int pos) {
		return HEADER_BYTES + pos * ITEM_BYTES;
	}
	
	private final int itemBytesStart() {
		return itemPosToAbsolute(itemsStart());
	}
	
	/**
	 * @return absolute byte position of items' end (exclusive)
	 */
	private final int itemBytesEnd() {
		return itemPosToAbsolute(itemsEnd());
	}
	
	private final int itemBytesLength() {
		return size() * ITEM_BYTES;
	}
	
	private final int colPtr(int pos){
		return bb.getInt(1 + INT_BYTES * pos);
	}
		
	private final long rowInd(int i){
		return bb.getLong(HEADER_BYTES + i * ITEM_BYTES);
	}
	
	private final float val(int i){
		return bb.getFloat(HEADER_BYTES + i * ITEM_BYTES + LONG_BYTES);
	}
	
	/**
	 * realtive put
	 * @param row
	 * @param val
	 */
	private final void putItem(long row, float val){
		bb.putLong(row).putFloat(val);
	}
}
