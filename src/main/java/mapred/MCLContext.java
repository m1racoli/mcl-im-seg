/**
 * 
 */
package mapred;

import io.writables.MCLMatrixSlice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Cedrik Neumann
 *
 */
public class MCLContext {

	private static final Logger logger = LoggerFactory.getLogger(MCLContext.class);

	private static boolean init = false;
	private static int kmax = MCLDefaults.kmax;
	private static long n = MCLDefaults.n;
	private static int nsub = MCLDefaults.nsub;
	private static Class<? extends MCLMatrixSlice<?>> matrixSliceClass = MCLDefaults.matrixSliceClass;
	private static int te = MCLDefaults.te;
	private static boolean varints = MCLDefaults.varints;	
	private static double inflation = MCLDefaults.inflation;
	private static float cutoff = MCLDefaults.cutoff;
	private static int cutoff_inv = MCLDefaults.cutoff_inv;
	private static int selection = MCLDefaults.selection;
	private static Class<? extends Selector> selectorClass = MCLDefaults.selectorClass;
	private static PrintMatrix print_matrix = MCLDefaults.printMatrix;
	private static boolean debug = false;
	
	public static final int getKMax(){
		return kmax;
	}

	public static final long getN() {
		return n;
	}

	public static final int getNSub(){
		return nsub;
	}

	@SuppressWarnings("unchecked")
	public static final <M extends MCLMatrixSlice<M>> Class<M> getMatrixSliceClass(){
		return (Class<M>) matrixSliceClass;
	}
	
	public static final <M extends MCLMatrixSlice<M>> M getMatrixSliceInstance(Configuration conf) {
		return ReflectionUtils.newInstance(MCLContext.<M>getMatrixSliceClass(), conf);
	}

	public static final int getNumThreads(){
		return te;
	}

	public static final boolean getUseVarints() {
		return varints;
	}
	
	public static final double getInflation(){
		return inflation;
	}

	/**
	 * @return max(p,1/P)
	 */
	public static final float getCutoff() {
		return Math.max(cutoff, 1.0f/cutoff_inv);
	}

	public static final int getSelection(){
		return selection;
	}

	@SuppressWarnings("unchecked")
	public static final <S extends Selector> Class<S> getSelectorClass() {
		return (Class<S>) selectorClass;
	}
	
	public static final <S extends Selector> S getSelectorInstance(Configuration conf) {
		return ReflectionUtils.newInstance(MCLContext.<S>getSelectorClass(), conf);
	}
	
	public static final PrintMatrix getPrintMatrix() {
		return print_matrix;
	}
	
	public static final boolean getDebug() {
		return debug;
	}
	
	public static final void init(Configuration conf) {
		if(init)
			return;
		
		init = true;
		kmax = MCLConfigHelper.getKMax(conf);
		n = MCLConfigHelper.getN(conf);
		nsub = MCLConfigHelper.getNSub(conf);
		matrixSliceClass = MCLConfigHelper.getMatrixSliceClass(conf);
		te = MCLConfigHelper.getNumThreads(conf);
		varints = MCLConfigHelper.getUseVarints(conf);
		inflation = MCLConfigHelper.getInflation(conf);
		cutoff = MCLConfigHelper.getCutoff(conf);
		cutoff_inv = MCLConfigHelper.getCutoffInv(conf);
		selection = MCLConfigHelper.getSelection(conf);
		selectorClass = MCLConfigHelper.getSelectorClass(conf);
		print_matrix = MCLConfigHelper.getPrintMatrix(conf);
		debug = MCLConfigHelper.getDebug(conf);
		
		if (debug) {
			org.apache.log4j.Logger.getLogger("mapred").setLevel(Level.DEBUG);
			org.apache.log4j.Logger.getLogger("io.writables").setLevel(Level.DEBUG);
			//TODO package
		}
	}
	
	public static final int getIdFromIndex(long idx, int nsub){
		return (int) (idx/nsub); //TODO tune
	}
	
	public static final int getSubIndexFromIndex(long idx, int nsub){
		return (int) (idx % nsub); //TODO tune
	}	
}
