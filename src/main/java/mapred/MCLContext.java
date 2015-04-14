/**
 * 
 */
package mapred;

import io.writables.MCLMatrixSlice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Level;

/**
 * old utility class
 * 
 * @author Cedrik Neumann
 *
 */
@Deprecated
public class MCLContext {

	//TODO MCL ConfigHelper
	public static final <M extends MCLMatrixSlice<M>> Class<M> getMatrixSliceClass(Configuration conf){
		return MCLConfigHelper.getMatrixSliceClass(conf);
	}
	
	//TODO MCL ConfigHelper
	public static final <M extends MCLMatrixSlice<M>> M getMatrixSliceInstance(Configuration conf) {
		return ReflectionUtils.newInstance(MCLContext.<M>getMatrixSliceClass(conf), conf);
	}
	
	public static final int getIdFromIndex(long idx, int nsub){
		return (int) (idx/nsub); //TODO tune
	}
	
	public static final int getSubIndexFromIndex(long idx, int nsub){
		return (int) (idx % nsub); //TODO tune
	}
	
	public static final void setLogging(Configuration conf){
		if (MCLConfigHelper.getDebug(conf)) {
			org.apache.log4j.Logger.getLogger("mapred").setLevel(Level.DEBUG);
			org.apache.log4j.Logger.getLogger("io").setLevel(Level.DEBUG);
			org.apache.log4j.Logger.getLogger("zookeeper").setLevel(Level.DEBUG);
		}
	}

}
