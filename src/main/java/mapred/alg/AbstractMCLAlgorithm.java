/**
 * 
 */
package mapred.alg;

import io.file.CSVWriter;
import io.file.TextFormatWriter;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import mapred.MCLConfigHelper;
import mapred.MCLDefaults;
import mapred.MCLOut;
import mapred.MCLResult;
import mapred.job.MCLStep;
import mapred.job.TransposeJob;
import mapred.job.input.NativeInputJob;
import mapred.job.output.ReadClusters;
import mapred.params.Applyable;
import mapred.params.MCLAlgorithmParams;
import mapred.params.MCLCompressionParams;
import mapred.params.MCLCoreParams;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.PathConverter;
import zookeeper.server.EmbeddedZkServer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * Core class of an MCL algortihm
 * 
 * @author Cedrik
 *
 */
public abstract class AbstractMCLAlgorithm extends Configured implements Tool {
	
	private static final Logger logger = LoggerFactory.getLogger(AbstractMCLAlgorithm.class);
	
	@Parameter(names = "-i", required = true, description = "folder of input matrix")
	private Path input = null;
	
	@Parameter(names = "-o", required = true, description = "output folder for the clustering result")
	private Path output = null;
	
	@Parameter(names = "--max-iter", description = "set max number of iterations")
	private int max_iterations = MCLDefaults.max_iterations;
	
	@Parameter(names = "--chaos-limit", description = "convergence treshold for chaos (MCL)")
	private double chaos_limit = MCLDefaults.chaosLimit;
	
	@Parameter(names = "--change-limit", description = "convergence treshold (R-MCL)")
	private double change_limit = MCLDefaults.changeLimit;
	
	private int min_iterations = MCLDefaults.min_iterations;
	
	@Parameter(names = {"-c","--counters"}, description = "write counters to file")
	private Path counters = null;
	private FileSystem countersFS = null;
	private TextFormatWriter countersWriter = null;
	
	@Parameter(names = {"-l","--logfile"}, description = "write output to logifle")
	private Path log = null;
	private FileSystem logFS = null;
	private FSDataOutputStream logStream = null;
	
	@Parameter(names = "--abc", description = "input matrix is in abc format (beta)")
	private boolean is_abc = false;
	
	@Parameter(names = "-zk", description = "run an embedded zookeeper server on <THIS_NODES_IP>:2181 for the distributed metrics")
	private boolean embeddedZkServer = false;
	
	@Parameter(names = "--force-iter", description = "force the number of iterations")
	private int fixed_iterations = 0;
	
	@Parameter(names = {"-h","--help"}, help = true, description = "show this help")
	private boolean help = false;
	
	@Parameter(names = {"--stats"}, description = "show more stats")
	private boolean stats = false;
	
	// further parameters
	private final MCLCoreParams coreParams = new MCLCoreParams();
	private final MCLAlgorithmParams params = new MCLAlgorithmParams();
	private final MCLCompressionParams compressionParams = new MCLCompressionParams();
	
	// path of the transposed used within the algorithm
	private Path transposePath = null;
	
	// implementation of transpose and mcl job
	private MCLOperation transposeJob = null;
	private MCLOperation stepJob = null;
	
	// counter for conducted iterations starting with the 1th iteration
	private int iteration = 1;
	
	/*
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public final int run(String[] args) throws Exception {
		
		// collection of parameter objects
		List<Object> params = new LinkedList<Object>();
		params.add(this);
		params.add(coreParams);
		params.add(this.params);
		params.add(compressionParams);
		
		// parse and evaluate parameters using JCommander
		JCommander cmd = new JCommander(params);
		cmd.addConverterFactory(new PathConverter.Factory());
		cmd.parse(args);
		
		if(help){
			// display help and exit
			cmd.usage();
			return 1;
		}
		
		// apply parameters to Configuration
		coreParams.apply(getConf());
		this.params.apply(getConf());
		compressionParams.apply(getConf());
		for(Applyable p : getParams()) p.apply(getConf());
		
		if (embeddedZkServer) {
			// launch embedded ZooKeeper server
			EmbeddedZkServer.init(getConf());
		}
		
		// use MapReduce implementations of the jobs
		transposeJob = new TransposeJob();
		stepJob = new MCLStep();
		
		if(MCLConfigHelper.hasNativeLib(getConf())){
			// if native library is provided (set by launch script)
			logger.debug("has native lib");
			getConf().set("mapreduce.map.child.java.opts", "-Djava.library.path=.");
		}
		
		// prepare output folder
		FileSystem outFS = output.getFileSystem(getConf());
		if (outFS.exists(output)) outFS.delete(output, true);
		outFS.mkdirs(output);
		
		// set transpose path
		transposePath = getTmp("t");
		
		// init
		initCounters();
		initLog();
		
		// call run method of algortihm implementation
		int rc = run(input, output);
		
		// cleanup
		closeCounters();
		closeLog();
		closeTmps();
		
		return rc;
	}
	
	/**
	 * override for more params which get applied to config
	 * @return additional params
	 */
	protected Collection<? extends Applyable> getParams() {
		return Collections.emptyList();
	}
	
	/**
	 * @return true if verbose stats
	 */
	protected final boolean showStats(){
		return stats;
	}
	
	/**
	 * run the algorithm
	 * 
	 * @param input path of input matrix
	 * @param output path of output clustering
	 * @return 0 if success
	 * @throws Exception
	 */
	protected abstract int run(Path input, Path output) throws Exception;
	
	/**
	 * initialize first iterant
	 * 
	 * @param input path of src matrix
	 * @param output path of first matrix
	 * @return MCLresult of job
	 * @throws Exception
	 */
	protected final MCLResult inputJob(Path input, Path output) throws Exception {
		MCLResult result = null;
		
		result = new NativeInputJob().run(getConf(), input, output);
		
		if (result == null || !result.success) {
			MCLOut.println("input failed");
			logger.error("failure! result = {}",result);
			return null;
		}
		
		return result;
	}
	
	/**
	 * perform block transpose
	 * 
	 * @param input path if input matrix
	 * @param output path of output block matrix
	 * @return MCLResult of the job
	 * @throws Exception
	 */
	protected final MCLResult transposeJob(Path input) throws Exception {
		
		logger.debug("run TransposeJob on {} => {}",input,transposePath);
		MCLResult result = transposeJob.run(getConf(), input, transposePath);
		writeCounters(result.counters,"transpose");
		
		if (result == null || !result.success) {
			MCLOut.println("transpose failed");
			logger.error("failure! result = {}",result);
			return null;
		}
		
		logger.info("{}",result);
		
		return result;
	}
	
	/**
	 * perform MCL step
	 * 
	 * @param paths list of input matrices (M_i,Rb,[M_i-1])
	 * @param output path of output matrix M_i+1
	 * @return MCLResult of the job
	 * @throws Exception
	 */
	protected final MCLResult stepJob(List<Path> paths, Path output) throws Exception{
		
		logger.debug("run MCLStep on {}  => {}",paths,output);		
		MCLResult result = stepJob.run(getConf(), paths, output);		
		writeCounters(result.counters,"step");
		
		if (result == null || !result.success) {
			MCLOut.println("step failed");
			logger.error("failure! result = {}",result);
			return null;
		}
		
		logger.info("{}",result);
		++iteration;
		return result;
	}
	
	/**
	 * interprete clusters from the final iterant
	 * 
	 * @param src path of the final iterant
	 * @param dest output of the clustering
	 * @return MCLResult of the job
	 * @throws Exception
	 */
	protected final MCLResult outputJob(Path src, Path dest) throws Exception {
		
		logger.debug("run ReadClusters: {} => {}",src,dest);
		ReadClusters readClusters = new ReadClusters();
		
		MCLResult result = readClusters.run(getConf(), src, dest);
		writeCounters(result.counters, "output");
		
		if (result == null || !result.success) {
			MCLOut.println("output failed");
			logger.error("failure! result = {}",result);
			return null;
		}
		
		logger.info("{}",result);
		return result;
	}
	
	/**
	 * @return maximum number iterations to perform
	 */
	protected final int getMaxIterations() {
		return max_iterations;
	}
	
	/**
	 * @return mininum number iterations to perform
	 */
	protected final int getMinIterations() {
		return min_iterations;
	}
	
	/**
	 * @return termination threshold for chaos
	 */
	protected final double getChaosLimit() {
		return chaos_limit;
	}
	
	/**
	 * @return termination threshold for change ratio
	 */
	protected final double getChangeLimit() {
		return change_limit;
	}

	/**
	 * @return path of the transposed
	 */
	protected final Path transposedPath(){
		return transposePath;
	}
	
	/**
	 * initialize counters output if set
	 * @throws IOException
	 */
	private final void initCounters() throws IOException {
		if(counters == null){
			return;
		}
		
		countersFS = counters.getFileSystem(getConf());
		counters = countersFS.makeQualified(counters);
		countersWriter = new CSVWriter(countersFS.create(counters, true));
		logger.info("log counters to {}",counters);
	}
	
	/**
	 * initialize log output if set
	 * @throws IOException
	 */
	private void initLog() throws IOException {
		if(log == null){
			return;
		}
		
		logFS = log.getFileSystem(getConf());
		log = logFS.makeQualified(log);
		logStream = logFS.create(log, true);
		logger.info("log output to {}",log);		
	}

	/**
	 * provide logfile output
	 * @return logfile outout, null if not exists
	 */
	protected FSDataOutputStream getLogStream(){
		return logStream;
	}
	
	/**
	 * write counters to counters output of exists
	 * @param counters to be written
	 * @param job name
	 * @throws IOException
	 */
	private final void writeCounters(Counters counters, String job) throws IOException {
		if(this.counters == null){
			return;
		}
		
		for (CounterGroup group : counters) {
			for (Counter counter : group) {
				countersWriter.write("iteration", iteration);
				countersWriter.write("job", job);
				countersWriter.write("group", group.getDisplayName());
				countersWriter.write("counter", counter.getDisplayName());
				countersWriter.write("value", counter.getValue());
				countersWriter.writeLine();
			}
		}
	}
	
	/**
	 * closes counters output if exists
	 * @throws IOException
	 */
	private final void closeCounters() throws IOException {
		if(counters == null){
			return;
		}
		
		countersWriter.close();
		countersFS.close();
	}
	
	/**
	 * closes the logfile output if exists
	 * @throws IOException
	 */
	private void closeLog() throws IOException {
		if(log == null){
			return;
		}
		
		logStream.close();
		logFS.close();
	}

	/**
	 * @return current iteration >= 1
	 */
	protected final int iter(){
		return iteration;
	}
	
	// tmp reference map and tmp file system
	private transient Map<String,Path> tmps = null; 
	private transient FileSystem tmpFS = null;
	
	/**
	 * create from tmp path from name or get reference of already created
	 * 
	 * @param name of the tmp path
	 * @return tmp path
	 * @throws IOException
	 */
	protected Path getTmp(String name) throws IOException{
		if(tmps == null){
			tmps = new LinkedHashMap<String, Path>();
			tmpFS = FileSystem.get(getConf());
		}
		
		Path tmp = tmps.get(name);
		
		if(tmp == null){
			tmp = new Path(tmpFS.getWorkingDirectory(),name);
			tmps.put(name, tmp);
			tmpFS.deleteOnExit(tmp);
			logger.info("tmp dir {} created",tmp);
		}
		
		return tmp;
	}
	
	/**
	 * cleanup tmp FilySystem and Path references
	 * 
	 * @throws IOException
	 */
	private void closeTmps() throws IOException{
		if(tmps == null){
			return;
		}
		
		tmpFS.close();
		tmps.clear();
		tmpFS = null;
		tmps = null;
	}
	
	/**
	 * @return number of fixed iterations to run, 0 otherwise
	 */
	protected final int getFixedIterations(){
		return fixed_iterations;
	}
	
}
