package io.writables;

import org.apache.hadoop.io.IntWritable;

public class SliceId extends IntWritable implements SliceIndex {

	@Override
	public int getSliceId() {
		return get();
	}

}
