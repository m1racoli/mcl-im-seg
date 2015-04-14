/**
 * 
 */
package mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;

/**
 * base instance class, from which subclasses can inherit parameters from the algorithms instance
 * 
 * @author Cedrik
 *
 */
public class MCLInstance extends MCLContext implements Configurable {
	
	protected boolean vint = MCLDefaults.varints;	
	protected int kmax = MCLDefaults.kmax;
	protected long n = MCLDefaults.n;
	protected int nsub = MCLDefaults.nsub;
	protected int te = MCLDefaults.te;
	
	protected final void writeLong(DataOutput out, long val) throws IOException {
		if(vint) WritableUtils.writeVLong(out, val);
		else out.writeLong(val);
	}
	
	protected final void writeInt(DataOutput out, int val) throws IOException {
		if(vint) WritableUtils.writeVInt(out, val);
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

	@Override
	public void setConf(Configuration conf) {
		vint = MCLConfigHelper.getUseVarints(conf);
		kmax = MCLConfigHelper.getKMax(conf);
		n = MCLConfigHelper.getN(conf);
		nsub = MCLConfigHelper.getNSub(conf);
		te = MCLConfigHelper.getNumThreads(conf);
		MCLContext.setLogging(conf);
	}

	@Override
	public Configuration getConf() {
		Configuration conf = new Configuration();
		MCLConfigHelper.setUseVarints(conf, vint);
		MCLConfigHelper.setKMax(conf, kmax);
		MCLConfigHelper.setN(conf, n);
		MCLConfigHelper.setNSub(conf, nsub);
		MCLConfigHelper.setNumThreads(conf, te);
		return conf;
	}
}
