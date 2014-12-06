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
public class BMCLJob extends AbstractMCLAlgorithm {

	private static final Logger logger = LoggerFactory.getLogger(BMCLJob.class);
	
	@Parameter(names = {"-b","--balance"})
	private double balance = 1.0;
	
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
		
		balance = Math.min(1.0, Math.max(0.0, balance));
		final int increment = balance == 0.0 ? 0 : (int) (1.0/balance);
		int weigth = 0;
		final boolean pure_mcl = balance == 1.0;
		
		MCLOut.init(getLogStream());
		MCLOut.start(n,MCLConfigHelper.getNSub(getConf()),MCLConfigHelper.getNumThreads(getConf()),result.kmax);
		
		long total_tic = System.currentTimeMillis();
		
		while(chaos >= getChaosLimit() && (pure_mcl || changeInNorm >= getChangeLimit()) && iter() <= getMaxIterations()){ //TODO chaos
			logger.debug("iteration i = {}",iter());
			MCLOut.startIteration(iter());
			
			long step_tic = System.currentTimeMillis();
			
			final boolean do_transpose = weigth <= 0;
			if(do_transpose){
				result = transposeJob(m_i_1);
				if(increment == 0) weigth = Integer.MAX_VALUE;
				else weigth += increment;
			}
			
			result = stepJob(iter() == 1 || pure_mcl ? Arrays.asList(m_i_1, transposedPath()) : Arrays.asList(m_i_1, transposedPath(), m_i_2), m_i);

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
					, (double) nnz_expand / (last_nnz + 1L)
					, (double) nnz_final  / (last_nnz + 1L)
					, (double) nnz_final  / (init_nnz + 1L)
					, (int) result.kmax);

			MCLOut.change(changeInNorm);
			if(do_transpose) MCLOut.transpose();
			MCLOut.finishIteration();
			weigth--;
		}
		
		long total_toc = System.currentTimeMillis() - total_tic;
		MCLOut.runningTime(total_toc);
		
		FileSystem fs = output.getFileSystem(getConf());
		Path res = new Path(output,"clustering");
		
		result = outputJob(m_i_1, res);
		
		MCLOut.clusters(result.clusters);
		
		//FileUtil.copy(fs, m_i_1, fs, res, true, true, getConf());
		fs.delete(m_i, true);
		fs.delete(m_i_1, true);
		fs.delete(m_i_2, true);
		fs.delete(transposedPath(), true);
		
		MCLOut.result(res);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new BMCLJob(), args));
	}

}
