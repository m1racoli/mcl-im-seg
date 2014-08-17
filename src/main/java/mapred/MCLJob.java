/**
 * 
 */
package mapred;

import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Cedrik
 *
 */
public class MCLJob extends AbstractMCLAlgorithm {

	@Override
	public int run(Path input, Path output) throws Exception {

		//TODO input job
		
		long n = 0; //TODO
		long converged_colums = 0;
		int i = 1;
		final Path transposed = suffix(input, "t");
		Path m_i_1 = input;
		TransposeJob transpose = new TransposeJob();
		MCLStep mclStep = new MCLStep();
		
		while(n > converged_colums && i < getMaxIterations()){
			Path m_i = suffix(input,i++);
			
			MCLResult transposeResult = transpose.run(getConf(), m_i_1, transposed);
			if (transposeResult == null || !transposeResult.success) {
				return 1;
			}
				
			
			MCLResult stepResult = mclStep.run(getConf(), Arrays.asList(m_i_1, transposed), m_i);
			
			if (stepResult == null || !stepResult.success) {
				return 1;
			}
			
			converged_colums = stepResult.homogenous_columns;			
			m_i_1 = m_i;
		}
		
		//TODO output path
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MCLJob(), args));
	}

}
