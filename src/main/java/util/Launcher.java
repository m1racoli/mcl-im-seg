/**
 * 
 */
package util;

import mapred.MCLJob;

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
		
		switch(args[0]){
		case "mcl":
			System.exit(ToolRunner.run(new MCLJob(), args));
		default:
			System.err.println("unknown command: "+args[0]);
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
