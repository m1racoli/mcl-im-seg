/**
 * 
 */
package mapred;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public abstract class AbstractMCLJob extends Configured implements Tool {
	
	@Parameter(names = "-i")
	private String input = null;
	
	@Parameter(names = "-o")
	private String output = null;
	
	@Parameter(names = "-cm")
	private boolean compress_map_output = false;
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public final int run(String[] args) throws Exception {
		JCommander cmd = new JCommander(this);
		cmd.addObject(MCLContext.instance);
		cmd.setAcceptUnknownOptions(false);
		cmd.parse(args);		
		
		Path inputPath = new Path(input);
		inputPath = inputPath.getFileSystem(getConf()).makeQualified(inputPath);
		
		Path outputPath = new Path(output);
		outputPath = outputPath.getFileSystem(getConf()).makeQualified(outputPath);
		
		getConf().setBoolean("mapreduce.compress.map.output", compress_map_output);
		
		int rc = algorithm(inputPath, outputPath);
		
		return rc;
	}
	
	public abstract int algorithm(Path input, Path output) throws Exception;
	
	public static final Path suffix(Path path, Object suffix){
		return new Path(path.getParent(),String.format("%s_%s", path.getName(),suffix));
	}

}
