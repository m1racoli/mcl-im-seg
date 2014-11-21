/**
 * 
 */
package mapred.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceEntry;
import io.writables.SliceId;
import mapred.MCLConfigHelper;
import mapred.MCLResult;
import math.ConnectedComponents;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Cedrik
 *
 */
public class ReadClusters extends AbstractMCLJob {

	private static final Logger logger = LoggerFactory.getLogger(ReadClusters.class);
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ReadClusters(), args));
	}

	private static final class ClusterMapper<M extends MCLMatrixSlice<M>> extends Mapper<SliceId,M,IntWritable,IntWritable>{
		
		private final IntWritable attractor = new IntWritable();
		private final IntWritable child = new IntWritable();		
		private int nsub;
		private long[] attIdx;
		private float[] attVal;
		
		protected void setup(Context context)
				throws IOException ,InterruptedException {
			nsub = MCLConfigHelper.getNSub(context.getConfiguration());
			attIdx = new long[nsub];
			attVal = new float[nsub];
		}
		
		@Override
		protected void map(SliceId key, M value, Context context)
				throws IOException, InterruptedException {
			
			Arrays.fill(attIdx, -1);
			Arrays.fill(attVal, 0.0f);
			
			final long shift = (long) nsub * (long) key.get();

			for(SliceEntry e : value.dump()){
				final int col = e.col;
				if(attVal[col] < e.val){
					attVal[col] = e.val;
					attIdx[col] = e.row;
				}
				
			}
			
			for(int col = nsub - 1; col >= 0; --col){
				final long att = attIdx[col];
				if(att == -1)
					continue;
				
				if(attVal[col] < 0.5f){
					context.getCounter(Counters.WEAK_ATTRACTORS).increment(1);
				} else {
					context.getCounter(Counters.STRONG_ATTRACTORS).increment(1);
				}
				child.set((int) (shift + col));
				attractor.set((int) att);
				//TODO long
				context.write(attractor, child);
			}
		}
		
	}
	
	private static final class ClusterReducer extends Reducer<IntWritable, IntWritable, Object, Object> {
		
		//TODO long
		private final Set<Integer> attractors = new HashSet<Integer>();
		private Map<Integer,List<Integer>> att_child_map = new LinkedHashMap<>();
		
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			Integer att = key.get();
			attractors.add(att);
		
			List<Integer> children = new ArrayList<>();
			
			for (IntWritable node : values) {
				
				Integer child = node.get();
				
				if(att.equals(child)){
					continue;
				}
				
				children.add(child);
			}
			
			att_child_map.put(att, children);
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			
			Map<Integer,Integer> attMap = attractorMapping(attractors, att_child_map);
			
			attractors.clear();
			
			Map<Integer,List<Integer>> new_att_child_map = new LinkedHashMap<>();
			
			for(Entry<Integer,List<Integer>> e : att_child_map.entrySet()){
				
				Integer att = attMap.containsKey(e.getKey()) ? attMap.get(e.getKey()) : e.getKey();
				
				List<Integer> list = new_att_child_map.get(att);
				if(list == null){
					new_att_child_map.put(att, e.getValue());
				} else {
					list.addAll(e.getValue());
				}
			}
			
			att_child_map.clear();
			
			context.getCounter(Counters.CLUSTERS).increment(new_att_child_map.size());
			
			for(Entry<Integer,List<Integer>> e : new_att_child_map.entrySet()){
				StringBuilder builder = new StringBuilder(e.getKey().toString());
				
				for(Integer i : e.getValue()){
					builder.append('\t').append(i);
				}
				
				context.write(builder, null);
			}			
		}
		
		private Map<Integer,Integer> attractorMapping(Set<Integer> att, Map<Integer,List<Integer>> att_child_map){

			Map<Integer,Integer> att_att_map = new LinkedHashMap<>(); //attractors mapping to their attractor
			
			for(Entry<Integer, List<Integer>> e : att_child_map.entrySet()){
				for(Integer node : e.getValue()){
					if(att.contains(node)){
						att_att_map.put(node,e.getKey());
					}
				}
			}
			
			if(att_att_map.isEmpty()){
				logger.info("no inter attractor relations");
				//no attractors mapping to others
				return att_att_map;
			}
			
			ConnectedComponents cc = new ConnectedComponents(att_att_map);
			
			Map<Integer,List<Integer>> cc_map = cc.compute();
			
			att_att_map.clear();
			
			for(Entry<Integer, List<Integer>> e : cc_map.entrySet()){
				for(Integer node : e.getValue()){
					att_att_map.put(node, e.getKey());
				}
			}			
			
			return att_att_map;
		}
	}
	
	private static enum Counters {
		CLUSTERS,SINGLE_NODE_CLUSTERS,ATTRACTORS_AS_CHILDREN, STRONG_ATTRACTORS, WEAK_ATTRACTORS
	}
	
	@Override
	protected MCLResult run(List<Path> inputs, Path output) throws Exception {
		
		MatrixMeta meta = MatrixMeta.load(getConf(), inputs.get(0));
		meta.apply(getConf());
		
		FileSystem fs = output.getFileSystem(getConf());
		fs.delete(output, true);
		
		Job job = Job.getInstance(getConf(), "Read Clusters");
		job.setJarByClass(ReadClusters.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inputs.get(0));
		
		job.setMapperClass(ClusterMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(ClusterReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, output);
		
		MCLResult result = new MCLResult();
		result.run(job);		
		return result;
	}
}
