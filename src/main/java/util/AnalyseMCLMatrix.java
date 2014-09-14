/**
 * 
 */
package util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Cedrik
 *
 */
public class AnalyseMCLMatrix extends AbstractUtil {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new AnalyseMCLMatrix(), args));
	}

	@Override
	protected int run(Path input, Path output, boolean hdfsOutput)
			throws Exception {

		FileSystem fs = FileSystem.getLocal(getConf());
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(input)));
		String line = null;
		while((line = reader.readLine()) != null){
			if(line.startsWith("dimension")){
				String[] dim = line.split(" ")[1].split("x");
				System.out.printf("dim: %s x %s\n", dim[0], dim[1]);
				continue;
			}
			
			if(line.equals("begin")){
				break;
			}
		}
		
		if(line == null){
			System.out.println("no begin found");
			return 1;
		}
		
		long nnz = 0;
		int kmax = 0;
		
		while((line = reader.readLine()) != null){
			
			if(line.equals(")")){
				break;
			}
			
			
			String[] column = line.split("[ \t]+");
			int k = column.length - 2;
			nnz += k;
			kmax = Math.max(kmax, k);
		}
		
		if(line == null){
			System.out.println("no end found");
			return 1;
		}
		
		reader.close();
		
		System.out.printf("nnz: %d\n", nnz);
		System.out.printf("kmax: %d\n", kmax);
		
		return 0;
	}

}
