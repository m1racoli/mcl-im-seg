/**
 * 
 */
package mapred;

import java.io.File;
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
		
		logger.debug("run InputJob on {} => {}",input,m_i_1);
		MCLResult result = new InputJob().run(getConf(), input, m_i_1);
		if (result == null || !result.success) {
			logger.error("failure! result = {}",result);
			return 1;
		}
		logger.info("{}",result);
		long n = result.n;
		long converged_colums = 0;
		
		File countersFile = null;
		
		if(dumpCounters()){
			countersFile = new File("counters.csv");
			result.prepareCounters(countersFile);
		}
		
		System.out.printf("n: %d, nsub: %d, paralellism: %d, nnz: %d, kmax: %d\n",n,MCLConfigHelper.getNSub(getConf()),MCLConfigHelper.getNumThreads(getConf()),result.nnz,result.kmax);
		System.out.println("iter\ttransp.\tstep\ttotal\tnnz\tkmax\tattractors\thom.col\tcutoff\tprune\tcputime");
		String outPattern = "%d\t%5.1f\t%5.1f\t%5.1f\t%9d\t%4d\t%9d\t%9d\t%9d\t%9d\t%5.1f\n";
		final Path transposed = suffix(input, "t");
		TransposeJob transpose = new TransposeJob();
		MCLStep mclStep = new MCLStep();
		long total_tic = System.currentTimeMillis();
		while(n > converged_colums && i < getMaxIterations()){
			
			logger.debug("iteration i = {}",i);
			Path m_i = suffix(input,i++);
			
			logger.debug("run TransposeJob on {} => {}",m_i_1,transposed);
			long step_tic = System.currentTimeMillis();
			result = transpose.run(getConf(), m_i_1, transposed);
			if (result == null || !result.success) {
				logger.error("failure! result = {}",result);
				return 1;
			}
			logger.info("{}",result);
			long transpose_millis = result.runningtime;
			logger.debug("run MCLStep on {} * {} => {}",m_i_1,transposed,m_i);
			if(dumpCounters()){
				result.dumpCounters(i-1, "transpose", countersFile);
			}
			
			result = mclStep.run(getConf(), Arrays.asList(m_i_1, transposed), m_i);
			long step_toc = System.currentTimeMillis() - step_tic;
			if (result == null || !result.success) {
				logger.error("failure! result = {}",result);
				return 1;
			}
			logger.info("{}",result);
			converged_colums = result.homogenous_columns;
			m_i_1 = m_i;
			if(dumpCounters()){
				result.dumpCounters(i-1, "step", countersFile);
			}
			
			System.out.printf(outPattern, i-1,transpose_millis/1000.0,result.runningtime/1000.0,step_toc/1000.0,
					result.nnz,result.kmax,result.attractors,result.homogenous_columns,result.cutoff,result.prune,result.cpu_millis/1000.0);
		}
		
		long total_toc = System.currentTimeMillis() - total_tic;
		System.out.printf("total runtime: %d seconds\n",total_toc/1000L);
		if(dumpCounters()) {
			System.out.println("counters written to "+countersFile.getAbsolutePath());
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MCLJob(), args));
	}

}
