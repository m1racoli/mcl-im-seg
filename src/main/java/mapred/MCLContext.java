/**
 * 
 */
package mapred;

import java.util.Map.Entry;

import io.writables.MCLMatrixSlice;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Cedrik Neumann
 *
 */
public class MCLContext implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger(MCLContext.class);

	private static Configuration conf = null;
	
	protected static final int getKMax(){
		return MCLConfigHelper.getKMax(conf);
	}

	protected static final long getN() {
		return MCLConfigHelper.getN(conf);
	}

	protected static final int getNSub(){
		return MCLConfigHelper.getNSub(conf);
	}

	//TODO MCL ConfigHelper
	public static final <M extends MCLMatrixSlice<M>> Class<M> getMatrixSliceClass(Configuration conf){
		return MCLConfigHelper.getMatrixSliceClass(conf);
	}
	
	//TODO MCL ConfigHelper
	public static final <M extends MCLMatrixSlice<M>> M getMatrixSliceInstance(Configuration conf) {
		return ReflectionUtils.newInstance(MCLContext.<M>getMatrixSliceClass(conf), conf);
	}

	protected static final int getNumThreads(){
		return MCLConfigHelper.getNumThreads(conf);
	}

	protected static final boolean getUseVarints() {
		return MCLConfigHelper.getUseVarints(conf);
	}
	
	protected static final double getInflation(){
		return MCLConfigHelper.getInflation(conf);
	}

	/**
	 * @return max(p,1/P)
	 */
	protected static final float getCutoff() {
		return Math.max(MCLConfigHelper.getCutoff(conf), 1.0f/MCLConfigHelper.getCutoffInv(conf));
	}

	protected static final int getSelection(){
		return MCLConfigHelper.getSelection(conf);
	}

	protected static final <S extends Selector> Class<S> getSelectorClass() {
		return MCLConfigHelper.getSelectorClass(conf);
	}
	
	protected static final <S extends Selector> S getSelectorInstance() {
		return ReflectionUtils.newInstance(MCLContext.<S>getSelectorClass(), conf);
	}
	
	protected static final PrintMatrix getPrintMatrix() {
		return MCLConfigHelper.getPrintMatrix(conf);
	}
	
	protected static final boolean getDebug() {
		return MCLConfigHelper.getDebug(conf);
	}
	
	public static final int getIdFromIndex(long idx, int nsub){
		return (int) (idx/nsub); //TODO tune
	}
	
	public static final int getSubIndexFromIndex(long idx, int nsub){
		return (int) (idx % nsub); //TODO tune
	}

	@Override
	public void setConf(Configuration conf) {
		if(MCLContext.conf == null) {
			MCLContext.conf = conf;
			
			if (getDebug()) {
				org.apache.log4j.Logger.getLogger("mapred").setLevel(Level.DEBUG);
				org.apache.log4j.Logger.getLogger("io.writables").setLevel(Level.DEBUG);
				//TODO package
				for(Entry<String, String> e : conf.getValByRegex("mcl.*").entrySet()){
					logger.debug("{}: {}",e.getKey(),e.getValue());
				}
			}
		}		
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}	
}
