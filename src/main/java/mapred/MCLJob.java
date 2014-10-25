/**
 * 
 */
package mapred;

import java.io.File;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
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

		File countersFile = null;
		
		if(dumpCounters()){
			countersFile = new File(System.getProperty("user.home")+"/counters.csv");
			MCLResult.prepareCounters(countersFile);
		}
		
		int i = 0;
		Path m_i_1 = new Path(output,"tmp_0"); //suffix(input,i++);
		Path m_i = new Path(output,"tmp_1");
		MCLResult result = null;
		
		if(!isNativeInput()){
			logger.debug("run InputJob on {} => {}",input,m_i_1);
			result = abc() 
					? new InputAbcJob().run(getConf(), input, m_i_1)
					: new SequenceInputJob().run(getConf(), input, m_i_1);
			
		} else {
			result = new NativeInputJob().run(getConf(), input, m_i_1);
		}
		
		if (result == null || !result.success) {
			logger.error("failure! result = {}",result);
			return 1;
		}
		
		logger.info("{}",result);
		long n = result.n;
		long converged_colums = 0;
		Long init_nnz = null; //result.out_nnz;
		
		System.out.printf("n: %d, nsub: %d, paralellism: %d, kmax: %d\n",n,MCLConfigHelper.getNSub(getConf()),MCLConfigHelper.getNumThreads(getConf()),result.kmax);
		MCLOut.init();
		final Path transposed = new Path(output, "t");
		TransposeJob transpose = new TransposeJob();
		MCLStep mclStep = new MCLStep();
		long total_tic = System.currentTimeMillis();
		while(n > converged_colums && ++i <= getMaxIterations()){ //TODO chaos
			logger.debug("iteration i = {}",i);
			MCLOut.startIteration(i);
			//Path m_i = suffix(input,i++);
			
			logger.debug("run TransposeJob on {} => {}",m_i_1,transposed);
			long step_tic = System.currentTimeMillis();
			result = transpose.run(getConf(), m_i_1, transposed);
			if (result == null || !result.success) {
				logger.error("failure! result = {}",result);
				return 1;
			}
			logger.info("{}",result);
			logger.debug("run MCLStep on {} * {} => {}",m_i_1,transposed,m_i);
			if(dumpCounters()){
				result.dumpCounters(i, "transpose", countersFile);
			}
			
			// MCL step
			result = mclStep.run(getConf(), Arrays.asList(m_i_1, transposed), m_i);
			long step_toc = System.currentTimeMillis() - step_tic;
			if (result == null || !result.success) {
				logger.error("failure! result = {}",result);
				return 1;
			}
			logger.info("{}",result);
			converged_colums = result.homogenous_columns;
			
			Path tmp = m_i_1;
			m_i_1 = m_i;
			m_i = tmp;
			
			if(dumpCounters()){
				result.dumpCounters(i-1, "step", countersFile);
			}
			MCLOut.progress(0.0f, 1.0f);
			
			if(init_nnz == null) init_nnz = result.in_nnz;
			final long last_nnz = result.in_nnz;
			final long nnz_final = result.out_nnz;
			final long nnz_expand = nnz_final + result.cutoff + result.prune;
			
			MCLOut.stats(result.chaos
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
		if(dumpCounters()) {
			System.out.println("counters written to "+countersFile.getAbsolutePath());
		}
		
		FileSystem fs = output.getFileSystem(getConf());
		Path res = new Path(output,"result");
		fs.rename(m_i_1, res);
		fs.delete(m_i, true);
		fs.delete(transposed, true);
		
		System.out.printf("Output written to: %s\n",res);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MCLJob(), args));
	}

}
