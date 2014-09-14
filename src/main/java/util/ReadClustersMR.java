/**
 * 
 */
package util;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceEntry;
import io.writables.SliceId;
import mapred.MCLConfigHelper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
public class ReadClustersMR extends AbstractUtil {

	private static final Logger logger = LoggerFactory.getLogger(ReadClustersMR.class);
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ReadClustersMR(), args));
	}

	private static final class ClusterMapper<M extends MCLMatrixSlice<M>> extends Mapper<SliceId,M,LongWritable,LongWritable>{
		
		private final LongWritable attractor = new LongWritable();
		private final LongWritable child = new LongWritable();		
		private long nsub;
		
		protected void setup(Context context)
				throws IOException ,InterruptedException {
			nsub = MCLConfigHelper.getNSub(context.getConfiguration());
		}
		
		@Override
		protected void map(SliceId key, M value,
				Mapper<SliceId, M, LongWritable, LongWritable>.Context context)
				throws IOException, InterruptedException {
			
			final long shift = nsub * (long) key.get();

			for(SliceEntry e : value.dump()){				
				child.set(shift + (long) e.col);
				attractor.set(e.row);				
				context.write(attractor, child);
			}
		}
		
	}
	
	private static final class ClusterReducer extends Reducer<LongWritable, LongWritable, Object, Object> {
		
		private final Set<Long> attractors = new HashSet<Long>();
		private final Set<Long> children = new HashSet<Long>();
		
		@Override
		protected void reduce(
				LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			
			final Long attractor = key.get();
			attractors.add(attractor);
			StringBuilder builder = new StringBuilder();
			builder.append(attractor);			
			boolean has_children = false;
			
			for (LongWritable node : values) {
				if(key.equals(node) || children.contains(node.get()))
					continue;
				
				has_children = true;
				children.add(node.get());
				builder.append('\t').append(node);
			}
			
			if(!has_children) context.getCounter(Counters.SINGLE_NODE_CLUSTERS).increment(1);
			context.getCounter(Counters.CLUSTERS).increment(1);
			context.write(builder, null);
		}
		
		@Override
		protected void cleanup(
				Reducer<LongWritable, LongWritable, Object, Object>.Context context)
				throws IOException, InterruptedException {
			children.retainAll(attractors);
			if(children.size() > 0){
				context.getCounter(Counters.ATTRACTORS_AS_CHILDREN).increment(children.size());
				logger.warn("attractors as children: {}",children.size());
			}			
		}
	}
	
	private static enum Counters {
		CLUSTERS,SINGLE_NODE_CLUSTERS,ATTRACTORS_AS_CHILDREN
	}
	
	@Override
	protected int run(Path input, Path output, boolean hdfsOutput) throws Exception {
		
		MatrixMeta meta = MatrixMeta.load(getConf(), input);
		meta.apply(getConf());
		
		output.getFileSystem(getConf()).delete(output, true);
		
		Job job = Job.getInstance(getConf(), "Read Clusters");
		job.setJarByClass(ReadClustersMR.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, input);
		
		job.setMapperClass(ClusterMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(ClusterReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
