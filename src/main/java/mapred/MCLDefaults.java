package mapred;

import io.writables.CSCSlice;
import io.writables.MCLMatrixSlice;

public class MCLDefaults {
	
	//matrix related
	//w/ value
	public static final int nsub = 128;
	public static final int te = 1;
	public static final boolean varints = false;
	public static final Class<? extends MCLMatrixSlice<?>> matrixSliceClass = CSCSlice.class;
	//w/o default value
	public static final int kmax = -1;
	public static final long n = -1;
	
	//generic
	public static final Class<? extends Selector> selectorClass = Selector.class;
	public static final PrintMatrix printMatrix = PrintMatrix.NNZ;
	
	//pruning
	public static final double inflation = 2.0;
	public static final float cutoff = 0.0001f;
	public static final int cutoff_inv = 10000;
	public static final int selection = 50;	
	public static final float pruneA = 0.9f;
	public static final float pruneB = 2.0f;
	
	//algorithm
	public static final int max_iterations = 100;
	public static final double chaosLimit = 0.0001;
	public static final double changeLimit = 0.0001;
	public static final boolean autoPrune = false;
	
}
