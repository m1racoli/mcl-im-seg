/**
 * 
 */
package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import io.cluster.ArrayClustering;
import io.cluster.Cluster;
import io.cluster.Clustering;
import io.cluster.ImageClusterings;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class Ncut extends AbstractUtil {
	
	@Parameter(names = "-te")
	private int te = 1;
	
	@Parameter()
	private List<String> inputs;
		
	/* (non-Javadoc)
	 * @see util.AbstractUtil#run(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, boolean)
	 */
	@Override
	protected int run(Path input, Path output, boolean hdfsOutput)
			throws Exception {

		if(inputs == null){
			System.out.println("no clusterings specified");
			System.exit(1);
		}
		
		String outputPath = output.toString();
		
		Map<Integer,Map<Integer,Double>> m = null;
		
		for(String cl : inputs){
			//System.out.println("load clustering");
			
			File clusteringFile = new File(cl);
			
			Clustering<Integer> clustering = new ArrayClustering(clusteringFile);
			int n = 0;
			for(Cluster<Integer> cluster : clustering){
				n += cluster.size();
			}
			
			if(m ==null)
				m = loadMatrix(new File(input.toString()),n);
			
			double ncut = ImageClusterings.nCut(clustering, m, te);
			System.out.printf("clusters: %d, total: %f  avg: %f\n",clustering.size(),ncut,ncut/clustering.size());
			ouput(new File(outputPath,clusteringFile.getName()), ncut);
		}
		
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Ncut(), args));
	}
	
	public static Map<Integer,Map<Integer,Double>> loadMatrix(File file, int n) throws IOException {
		Pattern pattern = Pattern.compile("\t");

		Map<Integer,Map<Integer,Double>> m = new HashMap<Integer, Map<Integer,Double>>(n,1.0f);
		
		BufferedReader reader = new BufferedReader(new FileReader(file));
		long cnt = 0;
		for(String line = reader.readLine(); line != null; line = reader.readLine()){
			String[] split = pattern.split(line);
			int col = Integer.parseInt(split[0]);
			int row = Integer.parseInt(split[1]);
			double v = Double.parseDouble(split[2]);
			Map<Integer,Double> map = m.get(row);
			if(map == null){
				map = new TreeMap<Integer, Double>();
				m.put(row, map);
			}
			map.put(col,v);
			cnt++;
		}
		
		reader.close();
		System.out.println("nnz: "+cnt);
		return m;
	}
	
	public static void ouput(File file, double ncut) throws IOException {
		BufferedWriter out = new BufferedWriter(new FileWriter(file));
		out.write(String.format(Locale.US, "%f", ncut));
		out.close();
	}

}
