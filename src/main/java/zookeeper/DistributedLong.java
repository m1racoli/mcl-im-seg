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
public abstract class DistributedLong implements DistributedMetric<DistributedLong> {

	private long val = 0;

	@Override
	public void clear() {
		val = 0;
	}
	
	public final long get(){
		return val;
	}
	
	public final void set(long val){
		val = merge(this.val,val);
	}
	
	@Override
	public void merge(DistributedLong v) {
		val = merge(this.val,v.val);
	}
	
	protected abstract long merge(long v1, long v2);
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(val);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		val = in.readLong();
	}

}
