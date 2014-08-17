package mapred;

import io.writables.MCLMatrixSlice;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

public class MCLInitParams implements IParams {

	private static final Logger logger = LoggerFactory.getLogger(MCLInitParams.class);
	
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
		
		if(logger.isDebugEnabled()) {
			logger.debug("nsub: {}",nsub);
			logger.debug("matrixSlice: {}",matrixSlice);
			logger.debug("te: {}",te);
			logger.debug("varints: {}",varints);
		}
	}
	
	
	
}
