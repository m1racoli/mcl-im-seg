package io.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mapred.MCLInstance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class SubBlock<M extends MCLMatrixSlice<M>> extends MCLInstance implements Writable {

	public int id = 0;
	public M subBlock = null;
	
	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		subBlock = getMatrixSliceInstance(conf);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		writeInt(out, id);
		subBlock.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = readInt(in);
		subBlock.readFields(in);
	}
	
	@Override
	public String toString() {
		return String.format("%d: %s", id, subBlock);
	}

}
