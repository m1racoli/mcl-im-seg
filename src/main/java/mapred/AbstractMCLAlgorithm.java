/**
 * 
 */
package mapred;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.PathConverter;
import zookeeper.server.EmbeddedZkServer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public abstract class AbstractMCLAlgorithm extends Configured implements Tool {
	
	private static final Logger logger = LoggerFactory.getLogger(AbstractMCLAlgorithm.class);
	
	@Parameter(names = "-i", required = true)
	private Path input = null;
	
	@Parameter(names = "-o", required = true)
	private Path output = null;
	
	@Parameter(names = "-debug")
	private boolean debug = false;
	
	@Parameter(names = "-verbose")
	private boolean verbose = false;
	
	@Parameter(names = "-iter")
	private int max_iterations = MCLDefaults.max_iterations;
	
	@Parameter(names = "--chaos-limit", description = "convergence treshold for chaos (MCL)")
	private double chaos_limit = MCLDefaults.chaosLimit;
	
	@Parameter(names = "--change-limit", description = "convergence treshold (R-MCL)")
	private double change_limit = MCLDefaults.changeLimit;
	
	@Parameter(names = "-dump-counters")
	private boolean dump_counters = false;
	
	@Parameter(names = "--abc")
	private boolean abc = false;
	
	@Parameter(names = "-zk")
	private boolean embeddedZkServer = false;
	
	@Parameter(names = "local")
	private boolean local = false;
	
	@Parameter(names = {"-native-input","-n"}, description= "input matrix is matrix slice") //TODO default
	private boolean native_input = false;
	
	private final MCLParams params = new MCLParams();
	private final MCLInitParams initParams = new MCLInitParams();
	private final MCLCompressionParams compressionParams = new MCLCompressionParams();
	
	private Path transposePath = null;
	private File countersFile = null;
	private TransposeJob transposeJob = new TransposeJob();
	private int transposeIter = 0;
	private int stepIter = 0;
	private MCLStep stepJob = new MCLStep();
	
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
		}
		
		if (embeddedZkServer) {
			EmbeddedZkServer.init(getConf());
		}
		
		FileSystem outFS = output.getFileSystem(getConf());
		if (outFS.exists(output)) {
			outFS.delete(output, true);
		}
		
		outFS.mkdirs(output);
		
		if(dumpCounters()){
			countersFile = new File(System.getProperty("user.home")+"/counters.csv");
			MCLResult.prepareCounters(countersFile);
		}
		
		transposePath = new Path(output,"t");
		
		int rc = run(input, output);
		
		if(rc != 0) return rc;
		
		if(dumpCounters()) {
			System.out.println("counters written to "+countersFile.getAbsolutePath());
		}
		
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
		
		if(!isNativeInput()){
			logger.debug("run InputJob on {} => {}",input,output);
			result = abc() 
					? new InputAbcJob().run(getConf(), input, output)
					: new SequenceInputJob().run(getConf(), input, output);
			
		} else {
			result = new NativeInputJob().run(getConf(), input, output);
		}
		
		if (result == null || !result.success) {
			logger.error("failure! result = {}",result);
			System.exit(1);
		}
		
		return result;
	}
	
	protected final MCLResult transposeJob(Path input) throws Exception {
		
		logger.debug("run TransposeJob on {} => {}",input,transposePath);
		MCLResult result = transposeJob.run(getConf(), input, transposePath);
		if (result == null || !result.success) {
			logger.error("failure! result = {}",result);
			System.exit(1);
		}
		
		logger.info("{}",result);
		
		if(dumpCounters()){
			result.dumpCounters(++transposeIter, "transpose", countersFile);
		}
		
		if(dumpCounters()){
			result.dumpCounters(++stepIter, "step", countersFile);
		}
		
		return result;
	}
	
	protected final MCLResult stepJob(List<Path> paths, Path output) throws Exception{
		
		logger.debug("run MCLStep on {}  => {}",paths,output);		
		MCLResult result = stepJob.run(getConf(), paths, output);
		
		if (result == null || !result.success) {
			logger.error("failure! result = {}",result);
			System.exit(1);
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
	
	public final double getChaosLimit() {
		return chaos_limit;
	}
	
	public final double getChangeLimit() {
		return change_limit;
	}
	
	public final boolean dumpCounters(){
		return dump_counters;
	}
	
	public final boolean abc(){
		return abc;
	}
	
	public final boolean isNativeInput(){
		return native_input;
	}

	public final Path transposedPath(){
		return transposePath;
	}
	
}
