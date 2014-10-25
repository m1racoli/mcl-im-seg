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
public class RMCLJob extends AbstractMCLAlgorithm {

	private static final Logger logger = LoggerFactory.getLogger(RMCLJob.class);
	
	@Override
	public int run(Path input, Path output) throws Exception {

		File countersFile = null;
		
		if(dumpCounters()){
			countersFile = new File(System.getProperty("user.home")+"/counters.csv");
			MCLResult.prepareCounters(countersFile);
		}
		
		int i = 0;
		
		Path m_i_2 = new Path(output,"tmp_0"); //suffix(input,i++);
		Path m_i_1 = new Path(output,"tmp_1");
		Path m_i   = new Path(output,"tmp_2");
		
		
		logger.debug("run InputJob on {} => {}",input,m_i_1);
		MCLResult result = abc() 
				? new InputAbcJob().run(getConf(), input, m_i_1)
				: new InputJob().run(getConf(), input, m_i_1);
		if (result == null || !result.success) {
			logger.error("failure! result = {}",result);
			return 1;
		}
		logger.info("{}",result);
		long n = result.n;
		long converged_colums = 0;
		
		System.out.printf("n: %d, nsub: %d, paralellism: %d, nnz: %d, kmax: %d\n",n,MCLConfigHelper.getNSub(getConf()),MCLConfigHelper.getNumThreads(getConf()),result.out_nnz,result.kmax);
		System.out.println("iter\tchaos\tstep\ttotal\tnnz\tkmax\tattractors\thom.col\tcutoff\tprune\tcputime\tchange");
		String outPattern = "%d\t%2.1f\t%5.1f\t%5.1f\t%9d\t%4d\t%9d\t%9d\t%9d\t%9d\t%5.1f\t%f\n";
		final Path transposed = new Path(output,"transposed");
		Path old = null;

		logger.debug("run TransposeJob on {} => {}",m_i_1,transposed);
		result = new TransposeJob().run(getConf(), m_i_1, transposed);
		if (result == null || !result.success) {
			logger.error("failure! result = {}",result);
			return 1;
		}
		logger.info("{}",result);
		long transpose_millis = result.runningtime;
		if(dumpCounters()){
			result.dumpCounters(i, "transpose", countersFile);
		}
		
		MCLStep mclStep = new MCLStep();
		long total_tic = System.currentTimeMillis();
		while(n > converged_colums && ++i <= getMaxIterations()){
			
			logger.debug("iteration i = {}",i);			
			
			long step_tic = System.currentTimeMillis();
			
			result = i == 1
					? mclStep.run(getConf(), Arrays.asList(m_i_1, transposed), m_i)
					: mclStep.run(getConf(), Arrays.asList(m_i_1, transposed, m_i_2), m_i);
			long step_toc = System.currentTimeMillis() - step_tic;
			if (result == null || !result.success) {
				logger.error("failure! result = {}",result);
				return 1;
			}
			logger.info("{}",result);
			converged_colums = result.homogenous_columns;
			
			Path tmp = m_i_1;
			m_i_1 = m_i;
			m_i = m_i_2;
			m_i_2 = tmp;
			
			if(dumpCounters()){
				result.dumpCounters(i, "step", countersFile);
			}
			
			System.out.printf(outPattern, i, result.chaos, result.runningtime/1000.0,step_toc/1000.0,
					result.out_nnz,result.kmax,result.attractors,result.homogenous_columns,result.cutoff,result.prune,result.cpu_millis/1000.0,result.changeInNorm);
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
		System.exit(ToolRunner.run(new RMCLJob(), args));
	}

}
