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
public class MCLParams {
	
	@Parameter(names = "-I")
	private double inflation = MCLDefaults.inflation;
	
	@Parameter(names = "-p")
	private float cutoff = MCLDefaults.cutoff;
	
	@Parameter(names = "-P")
	private int cutoff_inv = MCLDefaults.cutoff_inv;
	
	@Parameter(names = "-S")
	private int selection = MCLDefaults.selection;
	
	@Parameter(names = "-selector", converter = Selector.ClassConverter.class)
	private Class<? extends Selector> selectorClass = MCLDefaults.selectorClass;
	
	@Parameter(names = "-print-matrix")
	private PrintMatrix printMatrix = MCLDefaults.printMatrix;
	
	@Parameter(names = {"-a","--auto-prune"})
	private boolean auto_prune = MCLDefaults.autoPrune;
	
	public void apply(Configuration conf) {
		MCLConfigHelper.setInflation(conf, inflation);		
		
		if (cutoff == MCLDefaults.cutoff && cutoff_inv != MCLDefaults.cutoff_inv) {
			MCLConfigHelper.setCutoff(conf, 1.0f/cutoff_inv);
		} else {
			MCLConfigHelper.setCutoff(conf, cutoff);
		}
		
		MCLConfigHelper.setSelection(conf, selection);
		MCLConfigHelper.setSelectorClass(conf, selectorClass);
		MCLConfigHelper.setPrintMatrix(conf, printMatrix);
		MCLConfigHelper.setAutoPrune(conf, auto_prune);
	}
}