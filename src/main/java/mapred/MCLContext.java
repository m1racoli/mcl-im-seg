/**
 * 
 */
package mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
	
	@Parameter(names = "--nsub")
	private static int n_sub = 128;
	
	private static final String MCL_SUB_DIM = "mcl.sub.dim";
	
	@Parameter(names = "--slice-class")
	private static Class<? extends MCLMatrixSlice<?>> matrixSliceClass = CSCSlice.class;
	private static final String MCL_MATRIX_SLICE_CLASS = "mcl.matrix.slice.class";
	
	@Parameter(names = "--selector-class")
	private static Class<? extends Selector> selectorClass = Selector.class;
	private static final String MCL_SELECTOR_CLASS = "mcl.selector.class";
	
	@Parameter(names = "-te")
	private static int te = 1;
	private static final String MCL_NUM_THREADS = "mcl.num.threads";
	
	private static int k_max = 50;
	private static final String MCL_K_MAX = "mcl.k.max";
	
	@Parameter(names = "-vint")
	private static boolean vint = false; //TODO private final
	private static final String MCL_USE_VINT = "mcl.use.vint";
	
	@Parameter(names = "-print-matrix")
	private static String print_matrix = PrintMatrix.NNZ.toString();
	private static final String MCL_PRINT_MATRIX = "mcl.print.matrix";
	
	private static long n = 0;
	private static final String MCL_N = "mcl.n";
	
	public static PrintMatrix getPrintMatrix() {
		return PrintMatrix.valueOf(print_matrix);
	}

	public static void setPrintMatrix(PrintMatrix print_matrix) {
		MCLContext.print_matrix = print_matrix.toString();
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
		logger.debug("get {}: {}",MCL_INFLATION,I);
		P = conf.getInt(MCL_CUTOFF_INV, P);
		logger.debug("get {}: {}",MCL_CUTOFF_INV,P);
		p = conf.getFloat(MCL_CUTOFF, p);
		logger.debug("get {}: {}",MCL_CUTOFF,p);
		S = conf.getInt(MCL_SELECTION, S);
		logger.debug("get {}: {}",MCL_SELECTION, S);
		n_sub = conf.getInt(MCL_SUB_DIM, n_sub);
		logger.debug("get {}: {}",MCL_SUB_DIM, n_sub);
		matrixSliceClass = (Class<? extends MCLMatrixSlice<?>>) conf.getClass(MCL_MATRIX_SLICE_CLASS, matrixSliceClass, MCLMatrixSlice.class);
		logger.debug("get {}: {}",MCL_MATRIX_SLICE_CLASS, matrixSliceClass);
		selectorClass = conf.getClass(MCL_SELECTOR_CLASS, selectorClass, Selector.class);
		logger.debug("get {}: {}",MCL_SELECTOR_CLASS, selectorClass);
		te = conf.getInt(MCL_NUM_THREADS, te);
		logger.debug("get {}: {}",MCL_NUM_THREADS, te);
		k_max = conf.getInt(MCL_K_MAX, k_max);
		logger.debug("get {}: {}",MCL_K_MAX, k_max);
		n = conf.getLong(MCL_N, n);
		logger.debug("get {}: {}",MCL_N, n);
		vint = conf.getBoolean(MCL_USE_VINT, vint);
		logger.debug("get {}: {}",MCL_USE_VINT, vint);
		print_matrix = conf.get(MCL_PRINT_MATRIX, print_matrix);
		logger.debug("get {}: {}",MCL_PRINT_MATRIX, print_matrix);
	}
	
	public static final void set(Configuration conf){
		conf.setFloat(MCL_INFLATION, I);
		logger.debug("set {}: {}",MCL_INFLATION,I);
		conf.setInt(MCL_CUTOFF_INV, P);
		logger.debug("set {}: {}",MCL_CUTOFF_INV,P);
		conf.setFloat(MCL_CUTOFF, p);
		logger.debug("set {}: {}",MCL_CUTOFF,p);
		conf.setInt(MCL_SELECTION, S);
		logger.debug("set {}: {}",MCL_SELECTION, S);
		conf.setInt(MCL_SUB_DIM, n_sub);
		logger.debug("set {}: {}",MCL_SUB_DIM, n_sub);
		conf.setClass(MCL_MATRIX_SLICE_CLASS, matrixSliceClass, MCLMatrixSlice.class);
		logger.debug("set {}: {}",MCL_MATRIX_SLICE_CLASS, matrixSliceClass);
		conf.setClass(MCL_SELECTOR_CLASS, selectorClass, Selector.class);
		logger.debug("set {}: {}",MCL_SELECTOR_CLASS, selectorClass);
		conf.setInt(MCL_NUM_THREADS, te);
		logger.debug("set {}: {}",MCL_NUM_THREADS,te);
		conf.setInt(MCL_K_MAX, k_max);
		logger.debug("set {}: {}",MCL_K_MAX,k_max);
		conf.setLong(MCL_N, n);
		logger.debug("set {}: {}",MCL_N, n);
		conf.setBoolean(MCL_USE_VINT, vint);
		logger.debug("set {}: {}",MCL_USE_VINT, vint);
		conf.set(MCL_PRINT_MATRIX, print_matrix);
		logger.debug("set {}: {}",MCL_PRINT_MATRIX, print_matrix);
	}
	
	/**
	 * @return inflation parameter I
	 */
	public static final float getI(){
		return I;
	}
	
	public static final int getNSub(){
		return n_sub;
	}
	
	/**
	 * @return k_left * k_right
	 */
	public static final int getKMax(){
		return k_max;
	}
	
	public static final long getN() {
		return n;
	}
	
	public static final void setN(long n) {
		MCLContext.n = n;
	}
	
	public static final int getS(){
		return S;
	}
	
	public static final void setKMax(int k){
		k_max = k;
	}
	
	public static final void setNSub(int n_sub){
		MCLContext.n_sub = n_sub;
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
	public static final <M extends MCLMatrixSlice<M>> Class<M> getMatrixSliceClass(){
		return (Class<M>) matrixSliceClass;
	}
	
	public static final <M extends MCLMatrixSlice<M>> M getMatrixSliceInstance(Configuration conf) {
		return ReflectionUtils.newInstance(MCLContext.<M>getMatrixSliceClass(), conf);
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
	
	//TODO to MatrixSlice
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
		//logger.debug("col {} over n_sub {} -> id {}", idx, n_sub, )
		return (int) (idx/n_sub); //TODO tune
	}
	
	public static final int getSubIndexFromIndex(long idx){
		return (int) (idx % n_sub); //TODO tune
	}
	
	protected enum PrintMatrix {
		NNZ, COMPACT, ALL
	}
	
}
