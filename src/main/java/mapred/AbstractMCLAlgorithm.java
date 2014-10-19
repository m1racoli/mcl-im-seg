/**
 * 
 */
package mapred;

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
	
	@Parameter(names = "-cm")
	private boolean compress_map_output = false;
	
	@Parameter(names = "-debug")
	private boolean debug = false;
	
	@Parameter(names = "-verbose")
	private boolean verbose = false;
	
	@Parameter(names = "-iter")
	private int max_iterations = MCLDefaults.max_iterations;
	
	@Parameter(names = "-dump-counters")
	private boolean dump_counters = false;
	
	@Parameter(names = "--abc")
	private boolean abc = false;
	
	@Parameter(names = "-zk")
	private boolean embeddedZkServer = false;
	
	private final MCLParams params = new MCLParams();
	private final MCLInitParams initParams = new MCLInitParams();
	private final MCLCompressionParams compressionParams = new MCLCompressionParams();
	
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
		
		if (embeddedZkServer) {
			EmbeddedZkServer.init(getConf());
		}
		
		FileSystem outFS = output.getFileSystem(getConf());
		if (outFS.exists(output)) {
			outFS.delete(output, true);
		}
		
		outFS.mkdirs(output);
		
		return run(input, output);
	}
	
	/**
	 * override for more params which get applied to config
	 * @return additional params
	 */
	protected Collection<? extends Applyable> getParams() {
		return Collections.emptyList();
	}
	
	protected abstract int run(Path input, Path output) throws Exception;
	
	public static final Path suffix(Path path, Object suffix){
		return new Path(path.getParent(),String.format("%s_%s", path.getName(),suffix));
	}
	
	public final int getMaxIterations() {
		return max_iterations;
	}
	
	public final boolean dumpCounters(){
		return dump_counters;
	}
	
	public final boolean abc(){
		return abc;
	}

}
