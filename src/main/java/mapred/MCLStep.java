package mapred;

import java.io.IOException;

import io.writables.CSCSlice;
import io.writables.MCLMatrixSlice;

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

import zookeeper.DistributedLong;
import zookeeper.DistributedLongMaximum;
import zookeeper.ZkMetric;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

@SuppressWarnings("rawtypes")
public class MCLStep extends Configured implements Tool {
	
	private static final String K_MAX = "k_max";
	
	@Parameter(names = "-i")
	private String input = null;
	
	@Parameter(names = "-t")
	private String transposed = null;
	
	@Parameter(names = "-o")
	private String output = null;
	
	@Parameter(names = "-cm")
	private boolean compress_map_output = false;
	
	private static final class MCLMapper extends Mapper<LongWritable, TupleWritable, LongWritable, MCLMatrixSlice> {
		
		private final LongWritable id = new LongWritable();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			MCLContext.get(context);
		}
		
		@SuppressWarnings("unchecked")
		@Override
		protected void map(LongWritable key, TupleWritable tuple, Context context)
				throws IOException, InterruptedException {
			
			for(MCLMatrixSlice val : ((MCLMatrixSlice<?,?>) tuple.get(0)).getProductsWith(id,(MCLMatrixSlice) tuple.get(1))){
				context.write(id, val);
			}
		}
	}
	
	private static final class MCLCombiner extends Reducer<LongWritable, MCLMatrixSlice, LongWritable, MCLMatrixSlice> {
		
		private MCLMatrixSlice vec = null;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			MCLContext.get(context);
			if(vec == null){
				vec = MCLContext.getMatrixSliceInstance();
			}
		}
		
		@SuppressWarnings("unchecked")
		@Override
		protected void reduce(LongWritable col, Iterable<MCLMatrixSlice> values, Context context)
				throws IOException, InterruptedException {
			vec.combine(values,context);
			context.write(col, vec);
		}
	}
	
	public static final class MCLReducer extends Reducer<LongWritable, MCLMatrixSlice, LongWritable, MCLMatrixSlice> {		
		
		private MCLMatrixSlice vec = null;
		private DistributedLongMaximum kmax = new DistributedLongMaximum();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			MCLContext.get(context);
			if(vec == null){
				vec = MCLContext.getMatrixSliceInstance();
			}
		}
		
		@SuppressWarnings("unchecked")
		@Override
		protected void reduce(LongWritable col, Iterable<MCLMatrixSlice> values, Context context)
				throws IOException, InterruptedException {
			vec.combineAndProcess(values,context);
			kmax.set(vec.size());
			context.write(col, vec);
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			ZkMetric.set(context.getConfiguration(), K_MAX, kmax);
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
		
		ZkMetric.init(getConf(), K_MAX, true);
		
		int rc = job.waitForCompletion(false) ? 0 : 1;
		
		DistributedLong kmax = ZkMetric.get(getConf(), K_MAX);

		ZkMetric.close(getConf());
		return rc;
	}

	public static boolean run(Configuration conf, Path input, Path transposed, Path output){
		
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
