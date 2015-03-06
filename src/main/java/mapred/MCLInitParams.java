package mapred;

import io.writables.MCLMatrixSlice;

import org.apache.hadoop.conf.Configuration;
import com.beust.jcommander.Parameter;

public class MCLInitParams implements Applyable {
	
	@Parameter(names = "-nsub", description = "non native input: width of matrix slice")
	private int nsub = MCLDefaults.nsub;
	
	@Parameter(names = "-matrix-slice", converter = MCLMatrixSlice.ClassConverter.class, description = "non native input: matrix slice implementation")
	private Class<? extends MCLMatrixSlice<?>> matrixSlice = MCLDefaults.matrixSliceClass;
	
	@Parameter(names = "-native-matrix-slice", converter = MCLMatrixSlice.ClassConverter.class, description = "native input: matrix slice implementation")
	private Class<? extends MCLMatrixSlice<?>> nativeMatrixSlice = MCLDefaults.nativeMatrixSliceClass;
	
	@Parameter(names = "-te", description = "non native input: number of partitions the data is split into")
	private int te = MCLDefaults.te;
	
	@Parameter(names = "-vint", description = "non native input: variable length encoding for int and longs")
	private boolean varints = MCLDefaults.varints;
	
	@Parameter(names = {"-n","--native"}, description= "native computation engine")
	private boolean use_native = false;
	
	@Override
	public void apply(Configuration conf) {
		MCLConfigHelper.setNSub(conf, nsub);		
		MCLConfigHelper.setNumThreads(conf, te);
		MCLConfigHelper.setUseVarints(conf, varints);
		
		if(use_native || !nativeMatrixSlice.equals(MCLDefaults.nativeMatrixSliceClass)){
			MCLConfigHelper.setMatrixSliceClass(conf, nativeMatrixSlice);
		} else {
			MCLConfigHelper.setMatrixSliceClass(conf, matrixSlice);
		}
	}
}
