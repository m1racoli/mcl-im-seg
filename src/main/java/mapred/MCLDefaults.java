package mapred;

import io.writables.CSCSlice;
import io.writables.MCLMatrixSlice;
import io.writables.nat.NativeCSCSlice;

/**
 * 
 * all default values
 * 
 * @author Cedrik
 *
 */
public class MCLDefaults {
	
	//matrix related
	//w/ value
	public static final int nsub = 128;
	public static final int te = 1;
	public static final boolean varints = false;
	public static final Class<? extends MCLMatrixSlice<?>> matrixSliceClass = CSCSlice.class;
	public static final Class<? extends MCLMatrixSlice<?>> nativeMatrixSliceClass = NativeCSCSlice.class;
	
	//w/o default value
	public static final int kmax = -1;
	public static final long n = -1;
	
	//generic
	public static final PrintMatrix printMatrix = PrintMatrix.NNZ;
	public static final boolean local = false;
	
	//pruning
	public static final double inflation = 2.0;
	public static final float cutoff = 0.0001f;
	public static final int cutoff_inv = 10000;
	public static final int selection = 50;	
	public static final float pruneA = 0.9f;
	public static final float pruneB = 2.0f;
	
	//algorithm
	public static final int max_iterations = 10000;
	public static final double chaosLimit = 0.0001;
	public static final double changeLimit = 0.0001;
	public static final boolean autoPrune = false;
	public static final int min_iterations = 10;
	public static final boolean is_native = false;
	public static final boolean javaQueue = false;
	
	//framework
	public static final int nat_map_xmxm = 256;
	public static final int nat_reduce_xmxm = 256;
}
