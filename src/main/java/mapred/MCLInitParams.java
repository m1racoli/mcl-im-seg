package mapred;

import io.writables.MCLMatrixSlice;

import org.apache.hadoop.conf.Configuration;
import com.beust.jcommander.Parameter;

public class MCLInitParams implements Applyable {
	
	@Parameter(names = "-nsub")
	private int nsub = MCLDefaults.nsub;
	
	@Parameter(names = "-matrix-slice", converter = MCLMatrixSlice.ClassConverter.class)
	private Class<? extends MCLMatrixSlice<?>> matrixSlice = MCLDefaults.matrixSliceClass;
	
	@Parameter(names = "-te")
	private int te = MCLDefaults.te;
	
	@Parameter(names = "-vint")
	private boolean varints = MCLDefaults.varints;
	
	public void apply(Configuration conf) {
		MCLConfigHelper.setNSub(conf, nsub);
		MCLConfigHelper.setMatrixSliceClass(conf, matrixSlice);
		MCLConfigHelper.setNumThreads(conf, te);
		MCLConfigHelper.setUseVarints(conf, varints);
	}
}
