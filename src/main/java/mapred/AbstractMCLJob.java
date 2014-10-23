/**
 * 
 */
package mapred;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.PathConverter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public abstract class AbstractMCLJob extends Configured implements Tool {
	
	private static final Logger logger = LoggerFactory.getLogger(AbstractMCLJob.class);
	
	@Parameter(names = "-i", required = true)
	private List<Path> inputs = null;
	
	@Parameter(names = "-o", required = true)
	private Path output = null;
	
	@Parameter(names = "-debug")
	private boolean debug = false;
	
	@Parameter(names = "-verbose")
	private boolean verbose = false;
	
	@Parameter(names = "-local")
	private boolean local = false;
	
	private final MCLParams params = new MCLParams();
	private final MCLCompressionParams compressionParams = new MCLCompressionParams();
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public final int run(String[] args) throws Exception {
		
		List<Object> params = new LinkedList<Object>();
		params.add(this);
		params.add(this.params);
		params.add(this.compressionParams);
		
		//params.add(getParams());
		setCommander(params);
		JCommander cmd = new JCommander(params);
		cmd.addConverterFactory(new PathConverter.Factory());
		cmd.parse(args);
		
		this.params.apply(getConf());
		this.compressionParams.apply(getConf());
		
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
		
		FileSystem outFs = output.getFileSystem(getConf());
		if(outFs.exists(output)){
			outFs.delete(output, true);
		}
		
		MCLResult result = run(inputs, output);
		
		if (result == null) {
			logger.error("result == null");
			return -1;
		}
		
		System.out.println(result);
		
		return result.success ? 0 : 1;
	}
	
	public final MCLResult run(Configuration conf, Path input, Path output) throws Exception {
		return run(conf, Collections.singletonList(input), output);
	}
	
	public final MCLResult run(Configuration conf, List<Path> inputs, Path output) throws Exception {
		setConf(conf);
		FileSystem outFs = output.getFileSystem(getConf());
		if(outFs.exists(output)){
			outFs.delete(output, true);
		}
		return run(inputs, output);
	}	
	
	/**
	 * override for more params which get applied to config
	 * @return additional params
	 */
	protected Collection<? extends Applyable> getParams() {
		return Collections.emptyList();
	}
	
	protected void setCommander(List<Object> list){
		
	}
	
	protected abstract MCLResult run(List<Path> inputs, Path output) throws Exception;
	
}
