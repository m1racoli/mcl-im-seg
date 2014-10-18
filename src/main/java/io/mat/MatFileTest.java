/**
 * 
 */
package io.mat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import com.jmatio.io.MatFileHeader;
import com.jmatio.io.MatFileReader;
import com.jmatio.types.MLArray;
import com.jmatio.types.MLStructure;

/**
 * @author Cedrik
 *
 */
public class MatFileTest {

	//static final String filename = "C:\\other\\aufnahmen\\16112012_2.mat";
	static final String filename = "C:\\other\\aufnahmen";
	
	static final FilenameFilter matFilter = new FilenameFilter() {
		
		@Override
		public boolean accept(File dir, String name) {
			return name.endsWith(".mat");
		}
	};
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException {
		
		File dir = new File(filename);
		
		for(File file : dir.listFiles(matFilter)){
			MatFileReader reader = new MatFileReader(file);
			Map<String, MLArray> map = reader.getContent();
			
			if(!map.containsKey("ss0")){
				continue;
			}
			
			MLStructure ss0 = (MLStructure) map.get("ss0");
			
			int size = ss0.getSize();
			
			long fileSize = file.length();
			
			Set<String> fieldNames = new TreeSet<String>(ss0.getFieldNames());
			
			System.out.printf("%s\t%d\t%d\t%s\n", file.getName(), size, fileSize, fieldNames);
		}
		
	}

}
