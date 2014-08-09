/**
 * 
 */
package mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;

import io.writables.CSCSlice;
import io.writables.MCLMatrixSlice;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * @author Cedrik Neumann
 *
 */
public class MCLContext implements Configurable {

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
	private static int n_sub = 0;
	private static final String MCL_SUB_DIM = "mcl.sub.dim";
	
	@Parameter(names = "--slice-class")
	private static Class<? extends MCLMatrixSlice> matrixSliceClass = CSCSlice.class;
	private static final String MCL_MATRIX_SLICE_CLASS = "mcl.matrix.slice.class";
	
	@Parameter(names = "-te")
	private static int te = 1;
	private static final String MCL_NUM_THREADS = "mcl.num.threads";
	
	private static int k_left = 50;
	private static final String MCL_K_LEFT = "mcl.k.left";
	
	private static int k_right = 50;
	private static final String MCL_K_RIGHT = "mcl.k.right";
	
	@Parameter(names = "-vint")
	private static boolean vint = false;
	private static final String MCL_USE_VINT = "mcl.use.vint";
	
	private static boolean init_extended = false;

	public static void setInitExtended(boolean init_extended) {
		MCLContext.init_extended = init_extended;
	}

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
		matrixSliceClass = (Class<? extends MCLMatrixSlice>) conf.getClass(MCL_MATRIX_SLICE_CLASS, matrixSliceClass);
		logger.info("get {}: {}",MCL_MATRIX_SLICE_CLASS, matrixSliceClass);
		te = conf.getInt(MCL_NUM_THREADS, te);
		logger.info("get {}: {}",MCL_NUM_THREADS, te);
		k_left = conf.getInt(MCL_K_LEFT, k_left);
		logger.info("get {}: {}",MCL_K_LEFT, k_left);
		k_right = conf.getInt(MCL_K_RIGHT, k_right);
		logger.info("get {}: {}",MCL_K_RIGHT, k_right);
		vint = conf.getBoolean(MCL_USE_VINT, vint);
		logger.info("get {}: {}",MCL_USE_VINT, vint);
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
		conf.setInt(MCL_NUM_THREADS, te);
		logger.info("set {}: {}",MCL_NUM_THREADS,te);
		conf.setInt(MCL_K_LEFT, k_left);
		logger.info("set {}: {}",MCL_K_LEFT,k_left);
		conf.setInt(MCL_K_RIGHT, k_right);
		logger.info("set {}: {}",MCL_K_RIGHT,k_right);
		conf.setBoolean(MCL_USE_VINT, vint);
		logger.info("set {}: {}",MCL_USE_VINT, vint);
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
	
	public static final long getNSub(){
		return n_sub;
	}
	
	/**
	 * @return k_left * k_right
	 */
	public static final int getKMax(){
		return k_left*k_right;
	}
	
	public static final int getInitNnz(){
		return n_sub * (init_extended ?
				k_left * k_right : 
					k_left > k_right ? k_left : k_right);
	}
	
	public static final void setK(int k){
		setKLeft(k);
		setKRight(k);
	}
	
	public static final void setKLeft(int k){
		k_left = k;
	}
	
	public static final void setKRight(int k){
		k_right = k;
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
	
	public static final Class<? extends MCLMatrixSlice> getMatrixSliceClass(){
		return matrixSliceClass;
	}
	
	public static final MCLMatrixSlice getMatrixSubBlockInstance(Configuration conf) {
		return getMatrixSliceInstance(n_sub * k_right, conf);
	}
	
	public static final MCLMatrixSlice getExtendedMatrixSliceInstance(Configuration conf) {
		return getMatrixSliceInstance(n_sub * k_left * k_right, conf);
	}
	
	public static final MCLMatrixSlice getMatrixSliceInstance(Configuration conf){
		return getMatrixSliceInstance(n_sub * (k_left > k_right ? k_left : k_right), conf);
	}
	
	private static final MCLMatrixSlice getMatrixSliceInstance(int nnz, Configuration conf) {
		MCLMatrixSlice result;
		try {
			Constructor<? extends MCLMatrixSlice> meth = matrixSliceClass.getDeclaredConstructor(Integer.class);
			meth.setAccessible(true);
			result = meth.newInstance(nnz);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		result.setConf(conf);
		return result;
	}
	
	public static final int getNumThreads(){
		return te;
	}

	@Override
	public void setConf(Configuration conf) {
		get(conf);
	}

	@Override
	public Configuration getConf() {
		Configuration conf = new Configuration();
		set(conf);
		return conf;
	}
	
	protected static final void writeLong(DataOutput out, long val) throws IOException {
		if(vint) WritableUtils.writeVLong(out, val);
		else out.writeLong(val);
	}
	
	protected static final void writeInt(DataOutput out, int val) throws IOException {
		if(vint) WritableUtils.writeVLong(out, val);
		else out.writeInt(val);
	}
	
	protected static final long readLong(DataInput in) throws IOException {
		if(vint) return WritableUtils.readVLong(in);
		else return in.readLong();
	}
	
	protected static final int readInt(DataInput in) throws IOException {
		if(vint) return WritableUtils.readVInt(in);
		else return in.readInt();
	}
	
}
