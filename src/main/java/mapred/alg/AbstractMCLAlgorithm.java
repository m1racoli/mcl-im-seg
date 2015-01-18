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
import java.util.Map.Entry;

import mapred.Applyable;
import mapred.MCLCompressionParams;
import mapred.MCLConfigHelper;
import mapred.MCLDefaults;
import mapred.MCLInitParams;
import mapred.MCLOut;
import mapred.MCLParams;
import mapred.MCLResult;
import mapred.job.MCLStep;
import mapred.job.TransposeJob;
import mapred.job.input.InputAbcJob;
import mapred.job.input.NativeInputJob;
import mapred.job.input.SequenceInputJob;
import mapred.job.output.ReadClusters;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.PathConverter;
import zookeeper.server.EmbeddedZkServer;
import classic.InMemoryMCLStep;
import classic.InMemoryTransposeJob;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public abstract class AbstractMCLAlgorithm extends Configured implements Tool {
	
	private static final Logger logger = LoggerFactory.getLogger(AbstractMCLAlgorithm.class);
	
	@Parameter(names = "-i", required = true, description = "folder of input matrix")
	private Path input = null;
	
	@Parameter(names = "-o", required = true, description = "output folder for the clustering result")
	private Path output = null;
	
	@Parameter(names = "-debug", description = "DEBUG logging level for MCL")
	private boolean debug = false;
	
	@Parameter(names = "-verbose", description = "show MapReduce job progress")
	private boolean verbose = false;
	
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
	
	@Parameter(names = "--local", description = "run MapReduce in local mode")
	private boolean local = false;
	
	@Parameter(names = "--in-memory", description = "run in manual (non MapReduce) mode")
	private boolean in_memory;
	
	@Parameter(names = "--force-iter", description = "force the number of iterations")
	private int fixed_iterations = 0;
	
	@Parameter(names = {"-n","--native-input"}, description= "input matrix is matrix slice") //TODO default
	private boolean native_input = false;
	
	@Parameter(names = {"-h","--help"}, help = true, description = "show this help")
	private boolean help = false;
	
	private final MCLParams params = new MCLParams();
	private final MCLInitParams initParams = new MCLInitParams();
	private final MCLCompressionParams compressionParams = new MCLCompressionParams();
	
	private Path transposePath = null;
	
	private MCLOperation transposeJob = null;
	private MCLOperation stepJob = null;
	
	private int iteration = 1;
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public final int run(String[] args) throws Exception {
		
		List<Object> params = new LinkedList<Object>();
		params.add(this);
		params.add(this.params);
		params.add(initParams);
		params.add(compressionParams);
		params.add(getParams());
		JCommander cmd = new JCommander(params);
		cmd.addConverterFactory(new PathConverter.Factory());
		cmd.parse(args);
		
		if(help){
			cmd.usage();
			return 1;
		}
		
		this.params.apply(getConf());
		initParams.apply(getConf());
		compressionParams.apply(getConf());
		
		for(Applyable p : getParams()) {
			p.apply(getConf());
		}
		
		org.apache.log4j.Logger.getRootLogger().setLevel(Level.ERROR);
		
		if (verbose) {
			org.apache.log4j.Logger.getLogger(Job.class).setLevel(Level.INFO);
		}

		if (debug) {
			org.apache.log4j.Logger.getLogger("mapred").setLevel(Level.DEBUG);
			org.apache.log4j.Logger.getLogger("io.writables").setLevel(Level.DEBUG);
			org.apache.log4j.Logger.getLogger("zookeeper").setLevel(Level.DEBUG);			
			
			if(embeddedZkServer){
				//org.apache.log4j.Logger.getLogger("org.apache.zookeeper.server").setLevel(Level.DEBUG);
			}
			
			//TODO package
			MCLConfigHelper.setDebug(getConf(), true);
			for(Entry<String, String> e : getConf().getValByRegex("mcl.*").entrySet()){
				logger.debug("{}: {}",e.getKey(),e.getValue());
			}
		}
		
		if(local){
			logger.info("run mapreduce in local mode");
			getConf().set("mapreduce.framework.name", "local");
			getConf().set("yarn.resourcemanager.address", "local");
			MCLConfigHelper.setLocal(getConf(), local);
		}
		
		if (embeddedZkServer) {
			EmbeddedZkServer.init(getConf());
		}
		
		if(in_memory){
			transposeJob = new InMemoryTransposeJob();
			stepJob = new InMemoryMCLStep();
		} else {
			transposeJob = new TransposeJob();
			stepJob = new MCLStep();
		}
		
		FileSystem outFS = output.getFileSystem(getConf());
		if (outFS.exists(output)) {
			outFS.delete(output, true);
		}
		
		outFS.mkdirs(output);
		
		transposePath = getTmp("t");
		
		initCounters();
		
		initLog();
		
		int rc = run(input, output);
		
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
	
	protected abstract int run(Path input, Path output) throws Exception;
	
	protected final MCLResult inputJob(Path input, Path output) throws Exception {
		MCLResult result = null;
		
		if(!native_input){
			logger.debug("run InputJob on {} => {}",input,output);
			result = is_abc 
					? new InputAbcJob().run(getConf(), input, output)
					: new SequenceInputJob().run(getConf(), input, output);
			
		} else {
			result = new NativeInputJob().run(getConf(), input, output);
		}
		
		if (result == null || !result.success) {
			MCLOut.println("input failed");
			logger.error("failure! result = {}",result);
			return null;
		}
		
		return result;
	}
	
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
	
	public static final Path suffix(Path path, Object suffix){
		return new Path(path.getParent(),String.format("%s_%s", path.getName(),suffix));
	}
	
	public final int getMaxIterations() {
		return max_iterations;
	}
	
	public final int getMinIterations() {
		return min_iterations;
	}
	
	public final double getChaosLimit() {
		return chaos_limit;
	}
	
	public final double getChangeLimit() {
		return change_limit;
	}

	public final Path transposedPath(){
		return transposePath;
	}
	
	private final void initCounters() throws IOException {
		if(counters == null){
			return;
		}
		
		countersFS = counters.getFileSystem(getConf());
		counters = countersFS.makeQualified(counters);
		countersWriter = new CSVWriter(countersFS.create(counters, true));
		logger.info("log counters to {}",counters);
	}
	
	private void initLog() throws IOException {
		if(log == null){
			return;
		}
		
		logFS = log.getFileSystem(getConf());
		log = logFS.makeQualified(log);
		logStream = logFS.create(log, true);
		logger.info("log output to {}",log);		
	}

	protected FSDataOutputStream getLogStream(){
		return logStream;
	}
	
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
	
	private final void closeCounters() throws IOException {
		if(counters == null){
			return;
		}
		
		countersWriter.close();
		countersFS.close();
	}
	
	private void closeLog() throws IOException {
		if(log == null){
			return;
		}
		
		logStream.close();
		logFS.close();
	}

	/**
	 * current iteration >= 1
	 */
	protected final int iter(){
		return iteration;
	}
	
	private transient Map<String,Path> tmps = null;
	private transient FileSystem tmpFS = null;
	
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
	
	private void closeTmps() throws IOException{
		if(tmps == null){
			return;
		}
		
		tmpFS.close();
		tmps.clear();
		tmpFS = null;
		tmps = null;
	}
	
	public final int getFixedIterations(){
		return fixed_iterations;
	}
	
}
