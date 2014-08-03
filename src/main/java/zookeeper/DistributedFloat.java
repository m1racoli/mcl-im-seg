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
public abstract class DistributedFloat implements DistributedMetric<DistributedFloat> {

	private float val = 0;

	@Override
	public void clear() {
		val = 0.0f;
	}
	
	public final float get(){
		return val;
	}
	
	public final void set(float val){
		val = merge(this.val,val);
	}
	
	@Override
	public void merge(DistributedFloat v) {
		val = merge(this.val,v.val);
	}
	
	protected abstract float merge(float v1, float v2);
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(val);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		val = in.readFloat();
	}

}
