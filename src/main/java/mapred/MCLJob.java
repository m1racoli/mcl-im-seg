/**
 * 
 */
package mapred;

import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Cedrik
 *
 */
public class MCLJob extends AbstractMCLAlgorithm {

	private static final Logger logger = LoggerFactory.getLogger(MCLJob.class);
	
	@Override
	public int run(Path input, Path output) throws Exception {

		int i = 0;
		Path m_i_1 = suffix(input,i++);
		
		MCLResult result = new InputJob().run(getConf(), input, m_i_1);
		logger.info("{}",result);
		long n = result.n;
		long converged_colums = 0;
		
		final Path transposed = suffix(input, "t");
		TransposeJob transpose = new TransposeJob();
		MCLStep mclStep = new MCLStep();
		
		while(n > converged_colums && i < getMaxIterations()){
			Path m_i = suffix(input,i++);
			
			result = transpose.run(getConf(), m_i_1, transposed);
			if (result == null || !result.success) {
				return 1;
			}
				
			
			result = mclStep.run(getConf(), Arrays.asList(m_i_1, transposed), m_i);
			
			if (result == null || !result.success) {
				return 1;
			}
			logger.info("{}",result);
			converged_colums = result.homogenous_columns;
			m_i_1 = m_i;
		}
		
		//TODO output path
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MCLJob(), args));
	}

}
