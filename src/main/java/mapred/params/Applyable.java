/**
 * 
 */
package mapred.params;

import org.apache.hadoop.conf.Configuration;

/**
 * a configuration helper, which can apply settings to a Haddop Configuration
 * 
 * @author Cedrik
 *
 */
public interface Applyable {

	public void apply(Configuration conf);
	
}
