/**
 * 
 */
package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import io.cluster.ArrayClustering;
import io.cluster.Cluster;
import io.cluster.Clustering;
import mapred.util.FileUtil;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class StreamedNcut extends AbstractUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(StreamedNcut.class);
	
	@Parameter(names = "-te")
	private int te = 1;
	
	@Parameter()
	private List<String> inputList;
		
	/* (non-Javadoc)
	 * @see util.AbstractUtil#run(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, boolean)
	 */
	@Override
	protected int run(Path input, Path output, boolean hdfsOutput)
			throws Exception {

		if(inputList == null){
			logger.error("no clusterings specified");
			System.exit(1);
		}
		
		//final String outputPath = output.toString();
		final List<Clustering<Integer>> list = new ArrayList<Clustering<Integer>>(inputList.size());
		final List<Map<Cluster<Integer>,Double>> a1 = new ArrayList<Map<Cluster<Integer>,Double>>(inputList.size()); //assoc(A,A)
		final List<Map<Cluster<Integer>,Double>> a2 = new ArrayList<Map<Cluster<Integer>,Double>>(inputList.size()); //assoc(A,V)
		
		List<String> inputs = new ArrayList<String>();
		
		for(String in : inputList){
			logger.info("load {}",in);
			Path clusteringFile = new Path(in);
			FileSystem fs = clusteringFile.getFileSystem(getConf());
			
			if(fs.isDirectory(clusteringFile)){
				for(FileStatus status : fs.listStatus(clusteringFile, FileUtil.defaultPathFilter())){
					Path clFile = status.getPath();
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(clFile)));
					inputs.add(clFile.toString());
					Clustering<Integer> clustering = new ArrayClustering(reader);
					list.add(clustering);
					Map<Cluster<Integer>, Double> m1 = new HashMap<Cluster<Integer>, Double>(clustering.size(), 1.0f);
					Map<Cluster<Integer>, Double> m2 = new HashMap<Cluster<Integer>, Double>(clustering.size(), 1.0f);
					for(Cluster<Integer> cl : clustering){
						m1.put(cl, 0.0);
						m2.put(cl, 0.0);
					}
					a1.add(m1);
					a2.add(m2);
				}
			} else {
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(clusteringFile)));
				inputs.add(in);
				Clustering<Integer> clustering = new ArrayClustering(reader);
				list.add(clustering);
				Map<Cluster<Integer>, Double> m1 = new HashMap<Cluster<Integer>, Double>(clustering.size(), 1.0f);
				Map<Cluster<Integer>, Double> m2 = new HashMap<Cluster<Integer>, Double>(clustering.size(), 1.0f);
				for(Cluster<Integer> cl : clustering){
					m1.put(cl, 0.0);
					m2.put(cl, 0.0);
				}
				a1.add(m1);
				a2.add(m2);
			}
			
			
		}
		
		final Pattern pattern = Pattern.compile("\t");
		
		FileSystem fs = input.getFileSystem(getConf());
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(input)));
		for(String line = reader.readLine(); line != null; line = reader.readLine()){
			String[] split = pattern.split(line);
			final Integer col = Integer.valueOf(split[0]);
			final Integer row = Integer.valueOf(split[1]);
			final double v = Double.parseDouble(split[2]);
			
			int i = 0;
			for(Clustering<Integer> clustering : list){
				final Cluster<Integer> cl = clustering.getCluster(row);
				Map<Cluster<Integer>, Double> m2 = a2.get(i);
				m2.put(cl, v + m2.get(cl));
				if(cl.equals(clustering.getCluster(col))){
					Map<Cluster<Integer>, Double> m1 = a1.get(i);
					m1.put(cl, v + m1.get(cl));
				}
				i++;
			}
		}
		reader.close();
		
		for(int i = 0; i < list.size(); i++){
			Clustering<Integer> clustering = list.get(i);
			Map<Cluster<Integer>, Double> m1 = a1.get(i);
			Map<Cluster<Integer>, Double> m2 = a2.get(i);
			
			double nassoc = 0.0;
			Iterator<Double> i1 = m1.values().iterator();
			Iterator<Double> i2 = m2.values().iterator();
			while(i1.hasNext() && i2.hasNext()){
				nassoc += i1.next()/i2.next();
			}
			double ncut = clustering.size() - nassoc;
			logger.info("clustering: {}, size: {}, ncut: {}, avg ncut: {}",inputs.get(i),clustering.size(),ncut,ncut/clustering.size());
		}
		
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new StreamedNcut(), args));
	}
		
	public static void ouput(File file, double ncut) throws IOException {
		BufferedWriter out = new BufferedWriter(new FileWriter(file));
		out.write(String.format(Locale.US, "%f", ncut));
		out.close();
	}

}
