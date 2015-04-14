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
public abstract class DistributedInt implements DistributedMetric<DistributedInt> {

	private int val = 0;

	@Override
	public void clear() {
		val = 0;
	}
	
	public final int get(){
		return val;
	}
	
	public final void set(int val){
		this.val = merge(this.val,val);
	}
	
	@Override
	public void merge(DistributedInt v) {
		val = merge(this.val,v.val);
	}
	
	protected abstract int merge(int v1, int v2);
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(val);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		val = in.readInt();
	}
	
	@Override
	public String toString() {
		return String.valueOf(val);
	}

}
