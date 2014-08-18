/**
 * 
 */
package mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author Cedrik
 *
 */
public class MCLInstance extends MCLContext {

	protected boolean vint = MCLDefaults.varints;	
	protected int kmax = MCLDefaults.kmax;
	protected long n = MCLDefaults.n;
	protected int nsub = MCLDefaults.nsub;
	protected int te = MCLDefaults.te;
	
	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		vint = getUseVarints();
		kmax = getKMax();
		n = getN();
		nsub = getNSub();
		te = getNumThreads();
	}
	
	protected final void writeLong(DataOutput out, long val) throws IOException {
		if(vint) WritableUtils.writeVLong(out, val);
		else out.writeLong(val);
	}
	
	protected final void writeInt(DataOutput out, int val) throws IOException {
		if(vint) WritableUtils.writeVLong(out, val);
		else out.writeInt(val);
	}
	
	protected final long readLong(DataInput in) throws IOException {
		if(vint) return WritableUtils.readVLong(in);
		else return in.readLong();
	}
	
	protected final int readInt(DataInput in) throws IOException {
		if(vint) return WritableUtils.readVInt(in);
		else return in.readInt();
	}
	
	protected final int getIdFromIndex(long idx){
		return getIdFromIndex(idx, nsub);
	}
	
	protected final int getSubIndexFromIndex(long idx){
		return getSubIndexFromIndex(idx, nsub);
	}
}
