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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MCLStep extends AbstractMCLJob {
	
	private static final Logger logger = LoggerFactory.getLogger(MCLStep.class);
	
	private static final class MCLMapper<M extends MCLMatrixSlice<M>> extends Mapper<SliceId, TupleWritable, SliceId, M> {
		
		private final SliceId id = new SliceId();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
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
		
		if(inputs == null || inputs.size() < 2 || output == null){
			throw new RuntimeException(String.format("invalid input/output: in=%s, out=%s",inputs,output));
		}
		
		final Configuration conf = getConf();
		
		MatrixMeta meta = MatrixMeta.load(conf, inputs.get(0));
		MatrixMeta meta1 = MatrixMeta.load(conf, inputs.get(1));
		
		if(inputs.size() == 2){
			logger.debug("num inputs = 2. mcl step without comparison of iterants");
			MatrixMeta.check(meta,meta1);
			meta.setKmax(meta.getKmax() * meta1.getKmax());
		} else {
			logger.debug("num inputs > 2. mcl step with comparison of iterants");
			MatrixMeta meta2 = MatrixMeta.load(conf, inputs.get(2));
			MatrixMeta.check(meta,meta1,meta2);
			meta.setKmax(Math.max(meta.getKmax() * meta1.getKmax(),meta2.getKmax()));
		}
		
		meta.apply(conf);
		
		Job job = Job.getInstance(conf, "MCL Step");
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(CompositeInputFormat.class);
		job.getConfiguration().set(
				CompositeInputFormat.JOIN_EXPR,
				CompositeInputFormat.compose("inner", SequenceFileInputFormat.class, inputs.get(0), inputs.get(1)));
		
		job.setMapperClass(MCLMapper.class);
		job.setMapOutputKeyClass(SliceId.class);
		job.setMapOutputValueClass(CSCSlice.class);
		job.setOutputKeyClass(SliceId.class);
		job.setOutputValueClass(CSCSlice.class);
		job.setCombinerClass(MCLCombiner.class);
		job.setReducerClass(MCLReducer.class);
		job.setGroupingComparatorClass(IntWritable.Comparator.class);
		job.setNumReduceTasks(MCLConfigHelper.getNumThreads(conf));//TODO
		if(MCLConfigHelper.getNumThreads(conf) > 1) job.setPartitionerClass(SlicePartitioner.class);//TODO
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output);
		
		MCLResult result = new MCLResult();
		result.run(job);
		
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
