/**
 * 
 */
package mapred.alg;

import java.util.Arrays;

import mapred.MCLConfigHelper;
import mapred.MCLOut;
import mapred.MCLResult;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * implementation for both MCL and R-MCL algorithm using a balance parameter for the update ratio of the transpose block matrix.
 * 
 * @author Cedrik Neumann
 *
 */
public class BMCLJob extends AbstractMCLAlgorithm {

	private static final Logger logger = LoggerFactory.getLogger(BMCLJob.class);
	
	@Parameter(names = {"-b","--balance"})
	protected double balance = 1.0;
	
	@Override
	public int run(Path input, Path output) throws Exception {

		// tmp paths
		Path m_i_2 = getTmp("tmp_0");
		Path m_i_1 = getTmp("tmp_1");
		Path m_i   = getTmp("tmp_2");
		
		// init first iterant
		MCLResult inputResult = inputJob(input, m_i_1);
		if(inputResult == null) return 1;
		logger.info("{}",inputResult);
		
		long n = inputResult.n;
		double chaos = Double.MAX_VALUE;
		double changeInNorm = Double.POSITIVE_INFINITY;
		Long init_nnz = null;		
		balance = Math.min(1.0, Math.max(0.0, balance));
		final int increment = balance == 0.0 ? 0 : (int) (1.0/balance);
		int weigth = 0;
		final boolean pure_mcl = balance == 1.0;
		
		// init output stream
		MCLOut.init(getLogStream());
		// write header
		MCLOut.start(n, MCLConfigHelper.getNSub(getConf()), MCLConfigHelper.getNumThreads(getConf()), inputResult.kmax, showStats());
		
		long total_tic = System.currentTimeMillis();
		
		// iterate
		while( getFixedIterations() > 0 || (chaos >= getChaosLimit() && (pure_mcl || iter() <= getMinIterations() || changeInNorm >= getChangeLimit()) && iter() <= getMaxIterations())){
			logger.debug("iteration i = {}",iter());
			
			// log iteration start
			MCLOut.startIteration(iter());
			
			MCLResult transposeResult = null;
			MCLResult mclResult = null;
			
			long step_tic = System.currentTimeMillis();
			
			final boolean do_transpose = weigth <= 0;
			if(do_transpose){
				transposeResult = transposeJob(m_i_1);
				if(transposeResult == null) return 1;
				
				if(increment == 0) weigth = Integer.MAX_VALUE;
				else weigth += increment;
			}
			
			mclResult = stepJob(iter() == 1 || pure_mcl ? Arrays.asList(m_i_1, transposedPath()) : Arrays.asList(m_i_1, transposedPath(), m_i_2), m_i);
			if(mclResult == null) return 1;
			
			long step_toc = System.currentTimeMillis() - step_tic;
			chaos = mclResult.chaos;
			changeInNorm = mclResult.changeInNorm;
			
			Path tmp = m_i_2;
			m_i_2 = m_i_1;
			m_i_1 = m_i;
			m_i = tmp;

			MCLOut.progress(0.0f, 1.0f);
			
			if(init_nnz == null) init_nnz = mclResult.in_nnz;
			final long last_nnz = mclResult.in_nnz;
			final long nnz_final = mclResult.out_nnz;
			final long nnz_expand = nnz_final + mclResult.cutoff + mclResult.prune;
			
			MCLOut.stats(chaos
					, step_toc/1000.0
					, (double) nnz_expand / (last_nnz + 1L)
					, (double) nnz_final  / (last_nnz + 1L)
					, (double) nnz_final  / (init_nnz + 1L)
					, (int) mclResult.kmax);

			MCLOut.change(changeInNorm);
			MCLOut.transpose(do_transpose);
			
			if(showStats()){
				MCLOut.moreStats(transposeResult, mclResult);
			}
			
			MCLOut.finishIteration();
			weigth--;
			if(getFixedIterations() > 0 && getFixedIterations() < iter()){
				// fixed number of iterations reached
				break;
			}
		}
		
		long total_toc = System.currentTimeMillis() - total_tic;
		MCLOut.runningTime(total_toc);

		Path res = new Path(output,"clustering");
		
		MCLResult outResult = outputJob(m_i_1, res);
		if(outResult == null) return 1;
		
		MCLOut.clusters(outResult.clusters);
		MCLOut.result(res);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new BMCLJob(), args));
	}

}
