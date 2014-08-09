package io.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mapred.MCLContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class SubBlock extends MCLContext implements Writable {

	public int id = 0;
	public MCLMatrixSlice subBlock = null;
	
	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		subBlock = getMatrixSubBlockInstance(conf);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		writeLong(out, id);
		subBlock.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = readInt(in);
		subBlock.readFields(in);
	}

}
