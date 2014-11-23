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

import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class IMCLJob extends AbstractMCLAlgorithm {

	private static final Logger logger = LoggerFactory.getLogger(IMCLJob.class);
	
	@Parameter(names = "--factor")
	private double factor = 1.0;
	
	@Override
	public int run(Path input, Path output) throws Exception {

		Path m_i_2 = new Path(output,"tmp_0");
		Path m_i_1 = new Path(output,"tmp_1");
		Path m_i   = new Path(output,"tmp_2");	
		
		MCLResult result = inputJob(input, m_i_1);
		
		logger.info("{}",result);
		long n = result.n;
		double chaos = Double.MAX_VALUE;
		double changeInNorm = Double.POSITIVE_INFINITY;
		Long init_nnz = null; //result.out_nnz;
		double weigth = 0.0;
		
		factor = Math.min(1.0, Math.max(0.0, factor));
		
		System.out.printf("n: %d, nsub: %d, paralellism: %d, kmax: %d\n",n,MCLConfigHelper.getNSub(getConf()),MCLConfigHelper.getNumThreads(getConf()),result.kmax);
		MCLOut.init();
		
		long total_tic = System.currentTimeMillis();
		
		while(chaos >= getChaosLimit() && changeInNorm >= getChangeLimit() && iter() <= getMaxIterations()){ //TODO chaos
			logger.debug("iteration i = {}",iter());
			MCLOut.startIteration(iter());
			
			long step_tic = System.currentTimeMillis();
			
			if(weigth <= 0.0){
				result = transposeJob(m_i_1);
				weigth += 1.0;
			}			
			
			result = stepJob(iter() == 1 ? Arrays.asList(m_i_1, transposedPath()) : Arrays.asList(m_i_1, transposedPath(), m_i_2), m_i);	

			long step_toc = System.currentTimeMillis() - step_tic;
			chaos = result.chaos;
			changeInNorm = result.changeInNorm;
			
			Path tmp = m_i_2;
			m_i_2 = m_i_1;
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

			System.out.printf("\t%f",changeInNorm); //TODO not quick and dirty
			MCLOut.finishIteration();
			weigth += factor;
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
		System.exit(ToolRunner.run(new IMCLJob(), args));
	}

}
