/**
 * 
 */
package util;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public abstract class AbstractUtil extends Configured implements Tool {
	
	@Parameter(names = "-i", required = true)
	private Path input = null;
	
	@Parameter(names = "-o", required = true)
	private Path output = null;
	
	@Parameter(names = "-hdfs", description = "output to hdfs")
	private boolean hdfs;
	
	@Parameter(names = {"-h","--help"}, help = true)
	private boolean help;
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public final int run(String[] args) throws Exception {
		JCommander cmd = new JCommander(this);
		cmd.addConverterFactory(new PathConverter.Factory());
		cmd.parse(args);
		
		if(help){
			cmd.usage();
			System.exit(1);
		}
		
		return run(input, output, hdfs);
	}
	
	protected abstract int run(Path input, Path output, boolean hdfsOutput) throws Exception;

}
