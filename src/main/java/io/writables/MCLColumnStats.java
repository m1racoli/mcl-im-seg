/**
 * 
 */
package io.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author Cedrik
 *
 */
public class MCLColumnStats implements Writable {

	public final double[] center;
	
	/**
	 * 
	 */
	public MCLColumnStats(int columns) {
		center = new double[columns];
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for(int i = center.length; i > 0;){
			out.writeDouble(center[--i]);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		for(int i = center.length; i > 0;){
			center[--i] = in.readDouble();
		}
	}

}
