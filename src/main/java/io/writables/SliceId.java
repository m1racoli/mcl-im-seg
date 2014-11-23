package io.writables;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

public class SliceId extends IntWritable implements SliceIndex {

	public SliceId() {
		super();
	}
	
	public SliceId(int id){
		super(id);
	}
	
	@Override
	public int getSliceId() {
		return get();
	}
	
	static {
	    WritableComparator.define(SliceId.class, new Comparator());
	}

}
