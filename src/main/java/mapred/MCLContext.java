/**
 * 
 */
package mapred;

import io.writables.Column;
import io.writables.MCLMatrixSlice;
import io.writables.MCLSingleColumnMatrixSlice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * @author Cedrik Neumann
 *
 */
public class MCLContext {

	private static final Logger logger = LoggerFactory.getLogger(MCLContext.class);
	public static final MCLContext instance = new MCLContext();
	
	@Parameter(names = "-I")
	private static double I = 2.0;
	private static final String MCL_INFLATION = "mcl.inflation";
	
	@Parameter(names = "-P")
	private static int P = 100;
	private static final String MCL_CUTOFF_INV = "mcl.cutoff.inv";
	
	@Parameter(names = "-p")
	private static float p = 1.0f/1000.0f;
	private static final String MCL_CUTOFF = "mcl.cutoff";
	
	@Parameter(names = "-S")
	private static int S = 0; //TODO
	private static final String MCL_SELECTION = "mcl.selection";
	
	@Parameter(names = "-R")
	private static int R = 0; //TODO
	private static final String MCL_RECOVER = "mcl.recover";
	
	@Parameter(names = "-pct")
	private static int pct = 0; //TODO
	private static final String MCL_RECOVER_PCT = "mcl.recover.pct";
	
	@Parameter(names = "-n")
	private static long n = 0; //TODO auto
	private static final String MCL_DIM = "mcl.dim";
	
	@Parameter(names = "-nsub")
	private static long n_sub = 0;
	private static final String MCL_SUB_DIM = "mcl.sub.dim";
	
	@Parameter(names = "--slice-class")
	private static Class<? extends MCLMatrixSlice<?,?>> matrixSliceClass = Column.class;
	private static final String MCL_MATRIX_SLICE_CLASS = "mcl.matrix.slice.class";
	
	protected MCLContext(){}
	
	public static final void get(TaskInputOutputContext<?,?,?,?> context){
		get(context.getConfiguration());
	}
	
	/**
	 * loads MCLContext from the configuration
	 * 	
	 * @param conf
	 */
	@SuppressWarnings("unchecked")
	public static final void get(Configuration conf){
		I = conf.getDouble(MCL_INFLATION, I);
		logger.info("get {}: {}",MCL_INFLATION,I);
		P = conf.getInt(MCL_CUTOFF_INV, P);
		logger.info("get {}: {}",MCL_CUTOFF_INV,P);
		p = conf.getFloat(MCL_CUTOFF, p);
		logger.info("get {}: {}",MCL_CUTOFF,p);
		n = conf.getLong(MCL_DIM, n);
		logger.info("get {}: {}",MCL_DIM,n);
		matrixSliceClass = (Class<? extends MCLMatrixSlice<?, ?>>) conf.getClass(MCL_MATRIX_SLICE_CLASS, matrixSliceClass);
		logger.info("get {}: {}",MCL_MATRIX_SLICE_CLASS, matrixSliceClass);		
	}
	
	public static final void set(Configuration conf){
		conf.setDouble(MCL_INFLATION, I);
		logger.info("set {}: {}",MCL_INFLATION,I);
		conf.setInt(MCL_CUTOFF_INV, P);
		logger.info("set {}: {}",MCL_CUTOFF_INV,P);
		conf.setFloat(MCL_CUTOFF, p);
		logger.info("set {}: {}",MCL_CUTOFF,p);
		conf.setLong(MCL_DIM, n);
		logger.info("set {}: {}",MCL_DIM,n);
		conf.setClass(MCL_MATRIX_SLICE_CLASS, matrixSliceClass, MCLMatrixSlice.class);
		logger.info("set {}: {}",MCL_MATRIX_SLICE_CLASS, matrixSliceClass);
	}
	
	/**
	 * @return inflation parameter I
	 */
	public static final double getI(){
		return I;
	}
	
	/**
	 * @return dimension n of the matrix
	 */
	public static final long getDim(){
		return n;
	}
	
	public static final long getSubDim(){
		return n_sub;
	}
	
	/**
	 * @return k_max²
	 */
	public static final int getKMaxSquared(){
		return P*P;
	}
	
	/**
	 * @return inverse cutoff P == 1/p
	 */
	public static int getPInv() {
		return P;
	}

	/**
	 * @return cutoff p
	 */
	public static float getP() {
		return p;
	}

	
	public static final boolean isSingleColumnSlice(){
		return MCLSingleColumnMatrixSlice.class.isAssignableFrom(matrixSliceClass);
	}
	
	public static final Class<? extends MCLMatrixSlice<?,?>> getMatrixSliceClass(){
		return matrixSliceClass;
	}
	
	public static final MCLMatrixSlice<?,?> getMatrixSliceInstance(){
		return ReflectionUtils.newInstance(matrixSliceClass, null);
	}
	
}
