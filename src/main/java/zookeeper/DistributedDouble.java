/**
 * 
 */
package zookeeper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Cedrik
 *
 */
public abstract class DistributedDouble implements DistributedMetric<DistributedDouble> {

	private double val = 0.0;

	@Override
	public void clear() {
		val = 0.0;
	}
	
	public final double get(){
		return val;
	}
	
	/**
	 * applies the new value regarding the merging rule
	 * @param val
	 */
	public final void set(double val){
		this.val = merge(this.val,val);
	}
	
	@Override
	public void merge(DistributedDouble v) {
		this.val = merge(this.val,v.val);
	}
	
	protected abstract double merge(double v1, double v2);
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(val);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		val = in.readDouble();
	}
	
	@Override
	public String toString() {
		return String.valueOf(val);
	}
	
}
