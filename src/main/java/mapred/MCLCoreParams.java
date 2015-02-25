package mapred;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

public class MCLCoreParams implements Applyable {

	private static final Logger logger = LoggerFactory.getLogger(MCLCoreParams.class);
	
	@Parameter(names = {"-d","--debug"}, description = "DEBUG logging level for MCL")
	private boolean debug = false;
	
	@Parameter(names = {"-v","--verbose"}, description = "show MapReduce job progress")
	private boolean verbose = false;
	
	@Parameter(names = "--print-matrix", description = "define how a matrix slice should be printed to text (NNZ,COMPACT,ALL)")
	private PrintMatrix printMatrix = MCLDefaults.printMatrix;
	
	@Override
	public void apply(Configuration conf) {
		
		org.apache.log4j.Logger.getRootLogger().setLevel(Level.ERROR);
		
		if (verbose) {
			org.apache.log4j.Logger.getLogger(Job.class).setLevel(Level.INFO);
		}

		if (debug) {
			org.apache.log4j.Logger.getLogger("mapred").setLevel(Level.DEBUG);
			org.apache.log4j.Logger.getLogger("io.writables").setLevel(Level.DEBUG);
			org.apache.log4j.Logger.getLogger("zookeeper").setLevel(Level.DEBUG);
			//TODO package
			MCLConfigHelper.setDebug(conf, true);
			for(Entry<String, String> e : conf.getValByRegex("mcl.*").entrySet()){
				logger.debug("{}: {}",e.getKey(),e.getValue());
			}
		}
		
		MCLConfigHelper.setPrintMatrix(conf, printMatrix);
	}

}
