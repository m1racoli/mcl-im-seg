/**
 * 
 */
package mapred;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceEntry;
import io.writables.SliceId;

import org.apache.hadoop.fs.FileSystem;
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
public class ReadClusters extends AbstractMCLJob {

	private static final Logger logger = LoggerFactory.getLogger(ReadClusters.class);
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ReadClusters(), args));
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
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(ClusterReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, output);
		
		MCLResult result = new MCLResult();
		result.run(job);		
		return result;
	}
}
