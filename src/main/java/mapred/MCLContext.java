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
	private static float I = 2.0f;
	private static final String MCL_INFLATION = "mcl.inflation";
	
	@Parameter(names = "-P")
	private static int P = 10000;
	private static final String MCL_CUTOFF_INV = "mcl.cutoff.inv";
	
	@Parameter(names = "-p")
	private static float p = 1.0f/10000.0f;
	private static final String MCL_CUTOFF = "mcl.cutoff";
	
	@Parameter(names = "-S")
	private static int S = 50;
	private static final String MCL_SELECTION = "mcl.selection";
	
	@Parameter(names = "-n")
	private static long n = 0; //TODO auto
	private static final String MCL_DIM = "mcl.dim";
	
	@Parameter(names = "-nsub")
	private static int n_sub = 0;
	private static final String MCL_SUB_DIM = "mcl.sub.dim";
	
	@Parameter(names = "--slice-class")
	private static Class<? extends MCLMatrixSlice<?>> matrixSliceClass = CSCSlice.class;
	private static final String MCL_MATRIX_SLICE_CLASS = "mcl.matrix.slice.class";
	
	@Parameter(names = "--selector-class")
	private static Class<? extends Selector> selectorClass = null;
	private static final String MCL_SELECTOR_CLASS = "mcl.selector.class";
	
	@Parameter(names = "-te")
	private static int te = 1;
	private static final String MCL_NUM_THREADS = "mcl.num.threads";
	
	private static int k_left = 50;
	private static final String MCL_K_LEFT = "mcl.k.left";
	
	private static int k_right = 50;
	private static final String MCL_K_RIGHT = "mcl.k.right";
	
	@Parameter(names = "-vint")
	private static boolean vint = false; //TODO private final
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
		I = conf.getFloat(MCL_INFLATION, I);
		logger.info("get {}: {}",MCL_INFLATION,I);
		P = conf.getInt(MCL_CUTOFF_INV, P);
		logger.info("get {}: {}",MCL_CUTOFF_INV,P);
		p = conf.getFloat(MCL_CUTOFF, p);
		logger.info("get {}: {}",MCL_CUTOFF,p);
		S = conf.getInt(MCL_SELECTION, S);
		logger.info("get {}: {}",MCL_SELECTION, S);
		n = conf.getLong(MCL_DIM, n);
		logger.info("get {}: {}",MCL_DIM,n);
		n_sub = conf.getInt(MCL_SUB_DIM, n_sub);
		logger.info("get {}: {}",MCL_SUB_DIM, n_sub);
		matrixSliceClass = (Class<? extends MCLMatrixSlice<?>>) conf.getClass(MCL_MATRIX_SLICE_CLASS, matrixSliceClass, MCLMatrixSlice.class);
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
		conf.setFloat(MCL_INFLATION, I);
		logger.info("set {}: {}",MCL_INFLATION,I);
		conf.setInt(MCL_CUTOFF_INV, P);
		logger.info("set {}: {}",MCL_CUTOFF_INV,P);
		conf.setFloat(MCL_CUTOFF, p);
		logger.info("set {}: {}",MCL_CUTOFF,p);
		conf.setInt(MCL_SELECTION, S);
		logger.info("set {}: {}",MCL_SELECTION, S);
		conf.setLong(MCL_DIM, n);
		logger.info("set {}: {}",MCL_DIM,n);
		conf.setInt(MCL_SUB_DIM, n_sub);
		logger.info("set {}: {}",MCL_SUB_DIM, n_sub);
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
	public static final float getI(){
		return I;
	}
	
	/**
	 * @return dimension n of the matrix
	 */
	public static final long getDim(){
		return n;
	}
	
	public static final int getNSub(){
		return n_sub;
	}
	
	/**
	 * @return k_left * k_right
	 */
	public static final int getKMax(){
		return k_left*k_right;
	}
	
	public static final int getS(){
		return S;
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
	
	@SuppressWarnings("unchecked")
	public static final <M extends MCLMatrixSlice<?>> Class<M> getMatrixSliceClass(){
		return (Class<M>) matrixSliceClass;
	}
	
	public static final <M extends MCLMatrixSlice<?>> M getMatrixSubBlockInstance(Configuration conf) {
		return getMatrixSliceInstance(n_sub * k_right, conf);
	}
	
	public static final <M extends MCLMatrixSlice<?>> M getExtendedMatrixSliceInstance(Configuration conf) {
		return getMatrixSliceInstance(n_sub * k_left * k_right, conf);
	}
	
	public static final <M extends MCLMatrixSlice<?>> M getMatrixSliceInstance(Configuration conf){
		return getMatrixSliceInstance(n_sub * (k_left > k_right ? k_left : k_right), conf);
	}
	
	private static final <M extends MCLMatrixSlice<?>> M getMatrixSliceInstance(int nnz, Configuration conf) {
		M result;
		try {
			Constructor<M> meth = MCLContext.<M>getMatrixSliceClass().getDeclaredConstructor(Integer.class);
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
	
	public static final int getIdFromIndex(long idx){
		return (int) (idx/n_sub);
	}
	
	public static final int getSubIndexFromIndex(long idx){
		return (int) (idx % n_sub);
	}
	
	protected final int k_max = getKMax();
	
}
