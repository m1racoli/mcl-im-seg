package mapred;

import java.io.IOException;

import io.writables.CSCSlice;
import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceId;
import io.writables.SubBlock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zookeeper.DistributedLong;
import zookeeper.DistributedLongMaximum;
import zookeeper.ZkMetric;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class MCLStep extends Configured implements Tool {
	
	@Parameter(names = "-i")
	private String input = null;
	
	@Parameter(names = "-t")
	private String transposed = null;
	
	@Parameter(names = "-o")
	private String output = null;
	
	@Parameter(names = "-cm")
	private boolean compress_map_output = false;
	
	private static final class MCLMapper<M extends MCLMatrixSlice<M>> extends Mapper<SliceId, TupleWritable, SliceId, M> {
		
		private final SliceId id = new SliceId();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			MCLContext.get(context);
		}
		
		@SuppressWarnings("unchecked")
		@Override
		protected void map(SliceId key, TupleWritable tuple, Context context)
				throws IOException, InterruptedException {
			
			SubBlock<M> subBlock = (SubBlock<M>) tuple.get(1);
			id.set(subBlock.id);
			M m = (M) tuple.get(0);
			context.write(id,subBlock.subBlock.multipliedBy(m, context));
		}
	}
	
	private static final class MCLCombiner<M extends MCLMatrixSlice<M>> extends Reducer<SliceId, M, SliceId, M> {
		
		private M vec = null;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			MCLContext.get(context);
			if(vec == null){
				vec = MCLContext.<M>getMatrixSliceInstance(context.getConfiguration());
			}
		}
		
		@Override
		protected void reduce(SliceId col, Iterable<M> values, Context context)
				throws IOException, InterruptedException {
			vec.clear();
			for(M m : values){
				vec.add(m);
			}
			context.write(col, vec);
		}
	}
	
	public static final class MCLReducer<M extends MCLMatrixSlice<M>> extends Reducer<SliceId, M, SliceId, M> {		
		
		private M vec = null;
		private int k_max = 0;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			MCLContext.get(context);
			if(vec == null){
				vec = MCLContext.<M>getMatrixSliceInstance(context.getConfiguration());
			}
		}
		
		@Override
		protected void reduce(SliceId col, Iterable<M> values, Context context)
				throws IOException, InterruptedException {
			vec.clear();
			for(M m : values){
				vec.add(m);
			}
			
			k_max = Math.max(k_max, vec.inflateAndPrune(context));
			context.write(col, vec);
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			MatrixMeta.writeKmax(context, k_max);			
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		final Configuration conf = getConf();
		MCLContext.set(conf);
		
		final Path output = new Path(this.output);
		
		if(output.getFileSystem(conf).exists(output)){
			output.getFileSystem(conf).delete(output, true);
		}
		
		Job job = Job.getInstance(conf, "MCL Step");
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(CompositeInputFormat.class);
		job.getConfiguration().set(
				CompositeInputFormat.JOIN_EXPR,
				CompositeInputFormat.compose("inner", SequenceFileInputFormat.class, input, transposed));
		SequenceFileInputFormat.setInputPaths(job, input);
		
		job.setMapperClass(MCLMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(CSCSlice.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(CSCSlice.class);
		job.setCombinerClass(MCLCombiner.class);
		job.setReducerClass(MCLReducer.class);
		job.setGroupingComparatorClass(LongWritable.Comparator.class);
		job.setNumReduceTasks(MCLContext.getNumThreads());
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output);
		
		int rc = job.waitForCompletion(false) ? 0 : 1;
		
		return rc;
	}

	public static boolean run(Configuration conf, Path input, Path transposed, Path output){
		//TODO
		return false;
	}
	
	public static void main(String[] args) throws Exception {
		MCLStep job = new MCLStep();
		JCommander cmd = new JCommander();
		cmd.setAcceptUnknownOptions(true);
		cmd.addObject(job);
		cmd.addObject(MCLContext.instance);
		cmd.parse(args);
		int rc = ToolRunner.run(job, args);
		System.exit(rc);
	}
}
