/**
 * 
 */
package mapred;

import java.util.Collections;
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
import util.PathConverter.Factory;

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
	
	@Parameter(names = "-cm")
	private boolean compress_map_output = false;
	
	@Parameter(names = "-debug")
	private boolean debug = false;
	
	@Parameter(names = "-verbose")
	private boolean verbose = false;
	
	private final MCLParams params = new MCLParams();
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public final int run(String[] args) throws Exception {
		JCommander cmd = new JCommander(this);
		cmd.addConverterFactory(new PathConverter.Factory());
		cmd.addObject(params);
		for(Applyable params : getParams()){
			cmd.addObject(params);
		}
		cmd.parse(args);
		
		getConf().setBoolean("mapreduce.compress.map.output", compress_map_output);
		params.apply(getConf());
		
		for(Applyable params : getParams()) {
			params.apply(getConf());
		}
		
		org.apache.log4j.Logger.getRootLogger().setLevel(Level.WARN);
		
		if (verbose) {
			org.apache.log4j.Logger.getLogger(Job.class).setLevel(Level.INFO);
		}

		if (debug) {
			org.apache.log4j.Logger.getLogger("mapred").setLevel(Level.DEBUG);
			org.apache.log4j.Logger.getLogger("io.writables").setLevel(Level.DEBUG);
			//TODO package
			MCLConfigHelper.setDebug(getConf(), true);
			for(Entry<String, String> e : getConf().getValByRegex("mcl.*").entrySet()){
				logger.debug("{}: {}",e.getKey(),e.getValue());
			}
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
	protected Iterable<Applyable> getParams() {
		return Collections.emptyList();
	}
	
	protected abstract MCLResult run(List<Path> inputs, Path output) throws Exception;
	
}
