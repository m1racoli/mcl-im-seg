/**
 * 
 */
package util;

import java.util.Arrays;

import mapred.InputJob;
import mapred.MCLJob;
import mapred.TransposeJob;

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
		case "input":
			InputJob.main(args);
		case "transpose":
			TransposeJob.main(args);
		case "mcl":
			System.exit(ToolRunner.run(new MCLJob(), args));
		default:
			System.err.println("unknown command: "+command);
			errorReturn();
		}
	}
	
	private static void errorReturn(){
		System.err.print(usage());
		System.exit(1);
	}

	private static String usage() {
		return "mainClass <command> [args ...]\n";
	}
}
