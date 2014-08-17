/**
 * 
 */
package mapred;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Cedrik
 *
 */
public interface IParams {

	public void apply(Configuration conf);
	
}
