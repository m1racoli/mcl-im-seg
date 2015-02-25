/**
 * 
 */
package mapred;

import org.apache.hadoop.conf.Configuration;
import com.beust.jcommander.Parameter;

/**
 * @author Cedrik Neumann
 *
 */
public class MCLAlgorithmParams implements Applyable {
	
	@Parameter(names = "-I", description = "inflation parameter")
	private double inflation = MCLDefaults.inflation;
	
	@Parameter(names = "-p", description = "if manual pruning: set pruning threshold to p. overrides 1/P")
	private float cutoff = MCLDefaults.cutoff;
	
	@Parameter(names = "-P", description = "if manual pruning: set pruning threshold to 1/P if p is not set")
	private int cutoff_inv = MCLDefaults.cutoff_inv;
	
	@Parameter(names = "-S", description = "retain S highest entries during pruning")
	private int selection = MCLDefaults.selection;
	
	@Parameter(names = "-selector", converter = Selector.ClassConverter.class, description = "Java implementation for selection pruning")
	private Class<? extends Selector> selectorClass = MCLDefaults.selectorClass;
	
	@Parameter(names = {"-a","--auto-prune"}, description = "enable auto prune. overrides manual pruning options")
	private boolean auto_prune = MCLDefaults.autoPrune;
	
	@Override
	public void apply(Configuration conf) {
		MCLConfigHelper.setInflation(conf, inflation);		
		
		if (cutoff == MCLDefaults.cutoff && cutoff_inv != MCLDefaults.cutoff_inv) {
			MCLConfigHelper.setCutoff(conf, 1.0f/cutoff_inv);
		} else {
			MCLConfigHelper.setCutoff(conf, cutoff);
		}
		
		MCLConfigHelper.setSelection(conf, selection);
		MCLConfigHelper.setSelectorClass(conf, selectorClass);
		MCLConfigHelper.setAutoPrune(conf, auto_prune);
	}
}
