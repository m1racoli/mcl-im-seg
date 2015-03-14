/**
 * 
 */
package util;

import java.lang.reflect.Method;
import java.util.Arrays;

import mapred.alg.BMCLJob;
import mapred.alg.MCLJob;
import mapred.alg.RMCLJob;
import mapred.job.MCLStep;
import mapred.job.TransposeJob;
import mapred.job.input.InputAbcJob;
import mapred.job.input.InputJob;

import org.apache.hadoop.util.ToolRunner;

/**
 * @author Cedrik
 *
 */
public class Launcher {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if(args == null || args.length == 0){
			errorReturn();
		}
		
		String command = args[0];
		
		args = Arrays.copyOfRange(args, 1, args.length);
		
		switch(command){
		case "abc":
			InputAbcJob.main(args);
		case "input":
			InputJob.main(args);
		case "transpose":
			TransposeJob.main(args);
		case "step":
			MCLStep.main(args);
		case "mcl":
			System.exit(ToolRunner.run(new MCLJob(), args));
		case "rmcl":
			System.exit(ToolRunner.run(new RMCLJob(), args));
		case "bmcl":
			System.exit(ToolRunner.run(new BMCLJob(), args));
		case "test":
			TestRunner.main(args);
		case "load-img":
			System.exit(ToolRunner.run(new ImageTool(), args));
		case "load-mat":
			System.exit(ToolRunner.run(new MatTool(), args));
		case "result":
			VisualizeClusters.main(args);
		default:
			break;
		}
		
		Class<?> cls = Class.forName(command);
		Method meth = cls.getMethod("main", String[].class);
		meth.invoke(null, (Object) args);
	}
	
	private static void errorReturn(){
		System.err.print(usage());
		System.exit(1);
	}

	private static String usage() {
		return "mainClass <command> [args ...]\n";
	}
}
