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

import mapred.MCLConfigHelper;
import mapred.MCLStats;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Cedrik
 *
 */
public final class NativeArrayCSCSlice extends MCLMatrixSlice<NativeArrayCSCSlice> implements NativeSlice {

	private static final Logger logger = LoggerFactory.getLogger(NativeArrayCSCSlice.class);
	
	private static final byte TOP_ALIGNED = 0x00;
	private static final int INT_BYTES = Integer.SIZE/8;
	private static final int LONG_BYTES = Long.SIZE/8;
	private static final int ITEM_BYTES = LONG_BYTES + 2*Float.SIZE/8 ;

	private byte[] arr = null;
	private ByteBuffer bb = null;
	private int COLPTR_LAST;
	private int HEADER_BYTES;
	
	/**
	 * 
	 */
	public NativeArrayCSCSlice() {}
	
	public NativeArrayCSCSlice(Configuration conf){
		setConf(conf);
	}
	
	public NativeArrayCSCSlice(Configuration conf, int size){
		super.setConf(conf);
		init(conf, size);
	}
	
	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		init(conf, max_nnz);
	}
	
	private final void init(Configuration conf, int size){
		
		if(bb != null)
			return;
		
		NativeArrayCSCSliceHelper.setParams(nsub,select,auto_prune,inflation,cutoff,pruneA,pruneB,kmax,MCLConfigHelper.getDebug(conf));
		COLPTR_LAST = nsub * INT_BYTES + 1;
		HEADER_BYTES = INT_BYTES * (nsub + 1) + 1;
		
		arr = new byte[HEADER_BYTES + size * ITEM_BYTES];
		bb = ByteBuffer.wrap(arr);
		bb.order(ByteOrder.nativeOrder());
		clear();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.write(arr, 0, HEADER_BYTES);
		out.write(arr, itemBytesStart(), itemBytesLength());
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		in.readFully(arr, 0, HEADER_BYTES);
		in.readFully(arr, itemBytesStart(), itemBytesLength());
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#clear()
	 */
	@Override
	public void clear() {
		NativeArrayCSCSliceHelper.clear(arr);
	}

	private final void setAlignment(byte align){
		bb.put(0, align);
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
		
		setAlignment(TOP_ALIGNED);
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
			l++;
		}
		
		//logger.debug("{} items written",l);
		//logger.debug("position = {}",bb.position());
		
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
	public void add(NativeArrayCSCSlice m) {
		if(!NativeArrayCSCSliceHelper.add(arr, m.arr)){
			byte[] t = arr;
			arr = m.arr;
			m.arr = t;
			ByteBuffer tmp = bb;
			bb = m.bb;
			m.bb = tmp;
		}
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#multipliedBy(io.writables.MCLMatrixSlice, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public NativeArrayCSCSlice multipliedBy(NativeArrayCSCSlice m) {
		NativeArrayCSCSliceHelper.multiply(m.arr, arr);
		return this;
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#getSubBlockIterator(io.writables.SliceId)
	 */
	@Override
	protected ReadOnlyIterator<NativeArrayCSCSlice> getSubBlockIterator(SliceId id) {
		return new SubBlockIterator(id);
	}
	
	private final class SubBlockIterator extends ReadOnlyIterator<NativeArrayCSCSlice> {
		private final SliceId id;
		private final NativeArrayCSCSlice b = new NativeArrayCSCSlice(getConf(),nsub * (kmax < nsub ? kmax : nsub));
		private boolean has_next = NativeArrayCSCSliceHelper.startIterateBlocks(arr,b.arr);
		private boolean ready = true;
		
		public SubBlockIterator(SliceId id) {
			this.id = id;
		}
		
		@Override
		public boolean hasNext() {
			check();
			return has_next;
		}

		@Override
		public NativeArrayCSCSlice next() {
			check();
			ready = false;
			id.set(b.id());
			return b;
		}
		
		private final void check() {
			if(!ready){
				has_next = NativeArrayCSCSliceHelper.nextBlock();
				ready = true;
			}
		}
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#inflateAndPrune(mapred.MCLStats, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public void inflateAndPrune(MCLStats stats) {
		NativeArrayCSCSliceHelper.inflateAndPrune(arr, stats);
		logger.debug("returned stats: {}",stats);
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#makeStochastic(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public void makeStochastic() {
		NativeArrayCSCSliceHelper.makeStochastic(arr);
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof NativeArrayCSCSlice){
			NativeArrayCSCSlice o = (NativeArrayCSCSlice) obj;
			return nsub == o.nsub && NativeArrayCSCSliceHelper.equals(arr, o.arr);
		}
		return false;
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#addLoops(io.writables.SliceIndex)
	 */
	@Override
	public void addLoops(SliceIndex id) {
		NativeArrayCSCSliceHelper.addLoops(arr, id.getSliceId());
	}

	/* (non-Javadoc)
	 * @see io.writables.MCLMatrixSlice#sumSquaredDifferences(io.writables.MCLMatrixSlice)
	 */
	@Override
	public double sumSquaredDifferences(NativeArrayCSCSlice other) {
		return NativeArrayCSCSliceHelper.sumSquaredDifferences(arr, other.arr);
	}
	
	@Override
	public NativeArrayCSCSlice deepCopy() {
		NativeArrayCSCSlice other = getInstance();
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
		bb.putLong(row).putFloat(val).putInt(0);
	}
	
	private final int id(){
		return bb.getInt(itemBytesEnd());
	}
	
	@Override
	public String toString() {
		if(logger.isDebugEnabled()){
			int s = itemsStart();
			int t = itemsEnd();
			return String.format("NativeCSCSlice[bb: %s, mclSlice[align:%d, colPtr[0:[%d:%d] ... %d:[%d:%d]], items[0:[%d:(%d; %f)] ... %d:[%d:(%d; %f)]], %d]]",
					bb, bb.get(0),1,s,nsub,COLPTR_LAST,t, itemBytesStart(), rowInd(s), val(s), t-s-1, itemPosToAbsolute(t-1), rowInd(t-1),val(t-1),itemBytesEnd());
		}
		return super.toString();
	}

//	@Override
//	public String library() {
//		return NativeTest.NATIVE_TEST_LIB_NAME;
//	}
}
