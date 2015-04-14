package mapred.params;

import io.writables.MCLMatrixSlice;
import mapred.MCLConfigHelper;
import mapred.MCLDefaults;

import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.Parameter;

/**
 * parameters for the MatrixSlice instance
 * 
 * @author Cedrik
 *
 */
public class MCLInitParams implements Applyable {
	
	@Parameter(names = "-nsub", description = "width of matrix slice")
	private int nsub = MCLDefaults.nsub;
	
	@Parameter(names = "--matrix-slice", converter = MCLMatrixSlice.ClassConverter.class, description = "matrix slice implementation")
	private Class<? extends MCLMatrixSlice<?>> matrixSlice = MCLDefaults.matrixSliceClass;
	
	@Parameter(names = "-te", description = "number of partitions the data is split into")
	private int te = MCLDefaults.te;
	
	@Parameter(names = "-vint", description = "non native: variable length encoding for int and longs")
	private boolean varints = MCLDefaults.varints;
	
	@Parameter(names = {"-n","--native"}, description= "use NativeCSCSlice as matrix slice implementation")
	private boolean use_native = false;
	
	@Override
	public void apply(Configuration conf) {
		MCLConfigHelper.setNSub(conf, nsub);		
		MCLConfigHelper.setNumThreads(conf, te);
		MCLConfigHelper.setUseVarints(conf, varints);
		
		if(use_native){
			MCLConfigHelper.setMatrixSliceClass(conf, MCLDefaults.nativeMatrixSliceClass);
		} else {
			MCLConfigHelper.setMatrixSliceClass(conf, matrixSlice);
		}
		
		MCLConfigHelper.applyNative(conf);
	}
}
