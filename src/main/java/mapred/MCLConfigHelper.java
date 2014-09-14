/**
 * 
 */
package mapred;

import io.writables.MCLMatrixSlice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @author Cedrik
 *
 */
public class MCLConfigHelper {

	private static final String KMAX_CONF =			"mcl.matrix.kmax";
	private static final String N_CONF =			"mcl.matrix.n";
	private static final String NSUB_CONF =			"mcl.matrix.nsub";
	private static final String SLICE_CLS_CONF =	"mcl.matrix.slice.class";
	private static final String NUM_THREADS_CONF =	"mcl.num.threads";
	private static final String USE_VARINTS_CONF =	"mcl.use.varints";
	private static final String INFLATION_CONF = 	"mcl.inflation";
	private static final String CUTOFF_CONF =		"mcl.prune.cutoff";
	private static final String SELECTION_CONF =		"mcl.prune.selection";
	private static final String SELECTOR_CLS_CONF = "mcl.selector.class";
	private static final String PRINT_MATRIX_CONF =	"mcl.print.matrix";
	private static final String DEBUG_CONF =		"mcl.debug";
	
	public static final void setKMax(Configuration conf, int kmax) {
		conf.setInt(KMAX_CONF, kmax);
	}
	
	public static final int getKMax(Configuration conf) {
		return conf.getInt(KMAX_CONF, MCLDefaults.kmax);
	}
	
	public static final void setN(Configuration conf, long n) {
		conf.setLong(N_CONF, n);
	}
	
	public static final long getN(Configuration conf) {
		return conf.getLong(N_CONF, MCLDefaults.n);
	}
	
	public static final void setNSub(Configuration conf, int nsub) {
		conf.setInt(NSUB_CONF, nsub);
	}
	
	public static final int getNSub(Configuration conf) {
		return conf.getInt(NSUB_CONF, MCLDefaults.nsub);
	}
	
	public static final void setMatrixSliceClass(Configuration conf, Class<? extends MCLMatrixSlice<?>> cls){
		conf.setClass(SLICE_CLS_CONF, cls, MCLMatrixSlice.class);
	}
	
	@SuppressWarnings("unchecked")
	public static final <M extends MCLMatrixSlice<M>> Class<M> getMatrixSliceClass(Configuration conf){
		return (Class<M>) conf.getClass(SLICE_CLS_CONF, MCLDefaults.matrixSliceClass, MCLMatrixSlice.class);
	}
	
	public static final void setNumThreads(Configuration conf, int te) {
		conf.setInt(NUM_THREADS_CONF, te);
	}
	
	public static final int getNumThreads(Configuration conf) {
		return conf.getInt(NUM_THREADS_CONF, MCLDefaults.te);
	}
	
	public static final void setUseVarints(Configuration conf, boolean varints) {
		conf.setBoolean(USE_VARINTS_CONF, varints);
	}
	
	public static final boolean getUseVarints(Configuration conf) {
		return conf.getBoolean(USE_VARINTS_CONF, MCLDefaults.varints);
	}
	
	public static final void setInflation(Configuration conf, double inflation) {
		conf.setDouble(INFLATION_CONF, inflation);
	}
	
	public static final double getInflation(Configuration conf) {
		return conf.getDouble(INFLATION_CONF, MCLDefaults.inflation);
	}
	
	public static final void setCutoff(Configuration conf, float cutoff) {
		conf.setFloat(CUTOFF_CONF, cutoff);
	}
	
	public static final float getCutoff(Configuration conf) {
		return conf.getFloat(CUTOFF_CONF, MCLDefaults.cutoff);
	}
	
	public static final void setSelection(Configuration conf, int selection) {
		conf.setInt(SELECTION_CONF, selection);
	}
	
	public static final int getSelection(Configuration conf) {
		return conf.getInt(SELECTION_CONF, MCLDefaults.selection);
	}
	
	public static final <S extends Selector> void setSelectorClass(Configuration conf, Class<S> cls){
		conf.setClass(SELECTOR_CLS_CONF, cls, Selector.class);
	}
	
	@SuppressWarnings("unchecked")
	public static final <S extends Selector> Class<S> getSelectorClass(Configuration conf){
		return (Class<S>) conf.getClass(SELECTOR_CLS_CONF, MCLDefaults.selectorClass, Selector.class);
	}
	
	public static final void setPrintMatrix(Configuration conf, PrintMatrix printMatrix) {
		conf.setEnum(PRINT_MATRIX_CONF, printMatrix);
	}
	
	public static final PrintMatrix getPrintMatrix(Configuration conf) {
		return conf.getEnum(PRINT_MATRIX_CONF, MCLDefaults.printMatrix);
	}
	
	public static final void setDebug(Configuration conf, boolean debug) {
		conf.setBoolean(DEBUG_CONF, debug);
	}
	
	public static final boolean getDebug(Configuration conf) {
		return conf.getBoolean(DEBUG_CONF, false);
	}
	
	public static final <S extends Selector> S getSelectorInstance(Configuration conf) {
		return ReflectionUtils.newInstance(MCLConfigHelper.<S>getSelectorClass(conf), conf);
	}
}
