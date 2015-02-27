/**
 * 
 */
package mapred.job;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import mapred.Applyable;
import mapred.MCLCompressionParams;
import mapred.MCLCoreParams;
import mapred.MCLResult;
import mapred.alg.MCLOperation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
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
public abstract class AbstractMCLJob extends Configured implements Tool, MCLOperation {
	
	private static final Logger logger = LoggerFactory.getLogger(AbstractMCLJob.class);
	
	@Parameter(names = "-i", required = true, description = "input path")
	private List<Path> inputs = null;
	
	@Parameter(names = "-o", required = true, description = "output path")
	private Path output = null;
	
	@Parameter(names = "-zk")
	private boolean embeddedZkServer = false;
	
	@Parameter(names = {"--help"}, help = true)
	private boolean help = false;
	
	private final MCLCoreParams coreParams = new MCLCoreParams();
	private final MCLCompressionParams compressionParams = new MCLCompressionParams();
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public final int run(String[] args) throws Exception {
		
		List<Object> params = new LinkedList<Object>();
		params.add(this);
		params.add(this.coreParams);
		params.add(this.compressionParams);
		
//		for(Object o : getParams()){
//			params.add(o);
//		}
		
		//params.add(getParams());
		setCommander(params);
		JCommander cmd = new JCommander(params);
		cmd.addConverterFactory(new PathConverter.Factory());
		cmd.parse(args);
		
		if(help){
			cmd.usage();
			return 1;
		}
		
		this.coreParams.apply(getConf());
		this.compressionParams.apply(getConf());
		
		for(Applyable p : getParams()) {
			p.apply(getConf());
		}
		
		if (embeddedZkServer) {
			EmbeddedZkServer.init(getConf());
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
