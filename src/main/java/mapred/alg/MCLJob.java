/**
 * 
 */
package mapred.alg;

import java.util.Arrays;

import mapred.MCLConfigHelper;
import mapred.MCLOut;
import mapred.MCLResult;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * not up to date
 * 
 * @author Cedrik
 *
 */
public class MCLJob extends AbstractMCLAlgorithm {

	private static final Logger logger = LoggerFactory.getLogger(MCLJob.class);
	
	@Override
	public int run(Path input, Path output) throws Exception {

		Path m_i_1 = new Path(output,"tmp_0");
		Path m_i   = new Path(output,"tmp_1");
		
		MCLResult result = inputJob(input, m_i_1);
		
		logger.info("{}",result);
		long n = result.n;
		double chaos = Double.MAX_VALUE;
		Long init_nnz = null; //result.out_nnz;
		
		System.out.printf("n: %d, nsub: %d, paralellism: %d, kmax: %d\n",n,MCLConfigHelper.getNSub(getConf()),MCLConfigHelper.getNumThreads(getConf()),result.kmax);
		MCLOut.init();
		
		long total_tic = System.currentTimeMillis();
		
		
		while(chaos >= getChaosLimit() && iter() <= getMaxIterations()){ //TODO chaos
			logger.debug("iteration i = {}",iter());
			MCLOut.startIteration(iter());
			
			long step_tic = System.currentTimeMillis();
			
			result = transposeJob(m_i_1);
			result = stepJob(Arrays.asList(m_i_1, transposedPath()), m_i);

			long step_toc = System.currentTimeMillis() - step_tic;
			chaos = result.chaos;
			
			Path tmp = m_i_1;
			m_i_1 = m_i;
			m_i = tmp;

			MCLOut.progress(0.0f, 1.0f);
			
			if(init_nnz == null) init_nnz = result.in_nnz;
			final long last_nnz = result.in_nnz;
			final long nnz_final = result.out_nnz;
			final long nnz_expand = nnz_final + result.cutoff + result.prune;
			
			MCLOut.stats(chaos
					, step_toc/1000.0
					, 0.0
					, 0.0
					, 0.0
					, (double) nnz_expand / (last_nnz + 1L)
					, (double) nnz_final  / (last_nnz + 1L)
					, (double) nnz_final  / (init_nnz + 1L));

			MCLOut.finishIteration();
		}
		
		long total_toc = System.currentTimeMillis() - total_tic;
		System.out.printf("total runtime: %d seconds\n",total_toc/1000L);
		
		FileSystem fs = output.getFileSystem(getConf());
		Path res = new Path(output,"result");
		fs.rename(m_i_1, res);
		fs.delete(m_i, true);
		fs.delete(transposedPath(), true);
		
		System.out.printf("Output written to: %s\n",res);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MCLJob(), args));
	}

}
