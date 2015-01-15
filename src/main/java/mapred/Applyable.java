/**
 * 
 */
package mapred;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Cedrik
 *
 */
public interface Applyable {

	public void apply(Configuration conf);
	
}
