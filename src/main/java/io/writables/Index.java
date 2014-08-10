/**
 * 
 */
package io.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Cedrik
 *
 */
public class Index implements WritableComparable<Index>, SliceIndex {

	public final SliceId id = new SliceId();
	public final IntWritable col = new IntWritable();
	public final LongWritable row = new LongWritable();
	
	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		col.readFields(in);
		row.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		col.write(out);
		row.write(out);
	}

	@Override
	public int compareTo(Index o) {
		int cmp = id.compareTo(o.id);
		if(cmp != 0) return cmp;
		cmp = col.compareTo(o.col);
		if(cmp != 0) return cmp;
		return row.compareTo(o.row);
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}
	
	public boolean isDiagonal(){
		return row.get() == col.get();
	}

	@Override
	public String toString() {
		return String.format("(%d,%d,%d)", id, col, row);
	}
	
	public static final class Comparator extends WritableComparator{

		public Comparator() {super(Index.class);}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			long thisValue = readInt(b1, s1);
			long thatValue = readInt(b2, s2);
			if(thisValue != thatValue) return thisValue < thatValue ? -1 : 1;
			thisValue = readInt(b1, s1+4);
			thatValue = readInt(b2, s2+4);
			if(thisValue != thatValue) return thisValue < thatValue ? -1 : 1;
			thisValue = readLong(b1, s1+8);
			thatValue = readLong(b2, s2+8);
			return thisValue < thatValue ? -1 : (thisValue > thatValue ? 1 : 0);
		}		
	}
	
	static {
	    WritableComparator.define(Index.class, new Comparator());
	}

	@Override
	public int getSliceId() {
		return id.get();
	}
}
