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
	
	private boolean new_data = false;
	
	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		subBlock = getMatrixSliceInstance(conf);
	}
	
	public void setConf(Configuration conf, boolean init_slice) {
		super.setConf(conf);
		if (init_slice) subBlock = getMatrixSliceInstance(conf);
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
		new_data = true;
	}
	
	public boolean newData(){
		if(new_data){
			new_data = false;
			return true;
		}
		return false;		
	}
	
	@Override
	public String toString() {
		return String.format("%d: %s", id, subBlock);
	}

}
