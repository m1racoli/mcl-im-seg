package mapred.alg;

import java.util.List;

import mapred.MCLResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * an mcloperation generates output from one or more inputs
 * 
 * @author Cedrik
 *
 */
public interface MCLOperation {

	public MCLResult run(Configuration conf, Path input, Path output) throws Exception;
	
	public MCLResult run(Configuration conf, List<Path> inputs, Path output) throws Exception;
	
}
