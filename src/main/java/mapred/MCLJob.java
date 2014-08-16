/**
 * 
 */
package mapred;

import io.writables.MatrixMeta;

import org.apache.hadoop.fs.Path;

/**
 * @author Cedrik
 *
 */
public class MCLJob extends AbstractMCLJob {

	@Override
	public int algorithm(Path input, Path output) throws Exception {

		
		MatrixMeta meta = MatrixMeta.load(getConf(), input);
		
		long n = meta.n;
		long converged_colums = 0;
		int i = 1;
		final Path transposed = suffix(input, "t");
		Path m_i_1 = input;
		
		while(n > converged_colums){
			Path m_i = suffix(input,i++);
			
			if(!TransposeJob.run(getConf(), m_i_1, transposed))
				return 1;
			
			if(!MCLStep.run(getConf(), m_i_1, transposed, m_i))
				return 1;
			
			
			m_i_1 = m_i;
		}
		
		//TODO output path
		
		return 0;
	}

}
