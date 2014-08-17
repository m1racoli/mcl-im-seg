package mapred;

import java.io.IOException;
import java.util.List;

import io.writables.CSCSlice;
import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceId;
import io.writables.SubBlock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class MCLStep extends AbstractMCLJob {
	
	private static final class MCLMapper<M extends MCLMatrixSlice<M>> extends Mapper<SliceId, TupleWritable, SliceId, M> {
		
		private final SliceId id = new SliceId();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			MCLContext.init(context.getConfiguration());
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
			MCLContext.init(context.getConfiguration());
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
			MCLContext.init(context.getConfiguration());
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
	protected MCLResult run(List<Path> inputs, Path output) throws Exception {
		
		final Configuration conf = getConf();
		
		MatrixMeta meta0 = MatrixMeta.load(conf, inputs.get(0));
		MatrixMeta meta1 = MatrixMeta.load(conf, inputs.get(1));
		
		Job job = Job.getInstance(conf, "MCL Step");
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(CompositeInputFormat.class);
		job.getConfiguration().set(
				CompositeInputFormat.JOIN_EXPR,
				CompositeInputFormat.compose("inner", SequenceFileInputFormat.class, inputs.get(0), inputs.get(1)));
		
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
		
		MCLResult result = new MCLResult();
		result.success = job.waitForCompletion(true);
		
		meta.mergeKmax(conf, output);
		MatrixMeta.save(conf, output, meta);
		
		result.kmax = meta.getKmax();
		result.nnz = job.getCounters().findCounter(Counters.NNZ).getValue();
		result.attractors = job.getCounters().findCounter(Counters.ATTRACTORS).getValue();
		result.homogenous_columns = job.getCounters().findCounter(Counters.HOMOGENEOUS_COLUMNS).getValue();
		result.cutoff = job.getCounters().findCounter(Counters.CUTOFF).getValue();
		result.prune = job.getCounters().findCounter(Counters.PRUNE).getValue();		
		
		return result;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MCLStep(), args));
	}
}
