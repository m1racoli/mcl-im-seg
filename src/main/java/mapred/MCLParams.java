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
	
	public void apply(Configuration conf) {
		MCLConfigHelper.setInflation(conf, inflation);
		MCLConfigHelper.setCutoff(conf, cutoff);
		MCLConfigHelper.setCutoffInv(conf, cutoff_inv);
		MCLConfigHelper.setSelection(conf, selection);
		MCLConfigHelper.setSelectorClass(conf, selectorClass);
		MCLConfigHelper.setPrintMatrix(conf, printMatrix);
	}
}
