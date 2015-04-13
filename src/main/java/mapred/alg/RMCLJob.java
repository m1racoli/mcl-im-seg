/**
 * 
 */
package mapred.alg;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 * special case of bmcl algorithm with balance=0.0
 * 
 * @author Cedrik
 *
 */
public class RMCLJob extends BMCLJob {
	
	@Override
	public int run(Path input, Path output) throws Exception {
		
		balance = 0.0;
		return super.run(input, output);
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new RMCLJob(), args));
	}

}
