package mapred.job;

import java.io.IOException;
import java.util.List;

import io.writables.CSCSlice;
import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceId;
import io.writables.SubBlock;
import mapred.Counters;
import mapred.MCLConfigHelper;
import mapred.MCLContext;
import mapred.MCLResult;
import mapred.MCLStats;
import mapred.SlicePartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import zookeeper.DistributedDouble;
import zookeeper.DistributedDoubleMaximum;
import zookeeper.DistributedDoubleSum;
import zookeeper.DistributedInt;
import zookeeper.DistributedIntMaximum;
import zookeeper.ZkMetric;

public class MCLStep extends AbstractMCLJob {
	
	private static final Logger logger = LoggerFactory.getLogger(MCLStep.class);
	private static final String CHAOS = "/chaos";
	private static final String SSD = "/ssd";
	private static final String KMAX = "/kmax";	
	
	private static final class MCLMapper<M extends MCLMatrixSlice<M>> extends Mapper<SliceId, TupleWritable, SliceId, M> {
		
		private final SliceId id = new SliceId();
		private DistributedDouble ssd = null; //sum squared differences
		private int last_id = -1;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
		}
		
		@SuppressWarnings("unchecked")
		@Override
		protected void map(SliceId key, TupleWritable tuple, Context context)
				throws IOException, InterruptedException {
			
			M m = (M) tuple.get(0);
			
			if(last_id != key.get())
			{
				last_id = key.get();
				context.getCounter(Counters.MAP_INPUT_VALUES).increment(m.size());
				
				if(tuple.size() > 2)
				{					
					ssd = ssd == null ? new DistributedDoubleSum() : ssd;
					//if(ssd.get() < 1e-4f){ //break if treshold is already reached?
						ssd.set(m.sumSquaredDifferences((M) tuple.get(2)));
					//}				
				}
			}
			
			SubBlock<M> subBlock = (SubBlock<M>) tuple.get(1);
			id.set(subBlock.id);
			
			context.getCounter(Counters.MAP_INPUT_VALUES).increment(subBlock.subBlock.size());
			M product = subBlock.subBlock.multipliedBy(m, context);
			
			//count output records on diagonal and off diagonal
			if(id.get() == key.get()){
				context.getCounter(Counters.DIAG_MASS).increment(m.size());
			} else {
				context.getCounter(Counters.NON_DIAG_MASS).increment(m.size());
			}
			context.getCounter(Counters.MAP_OUTPUT_VALUES).increment(product.size());
			context.write(id, product);
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			if(ssd != null){
				ZkMetric.set(context.getConfiguration(), SSD, ssd);
				ZkMetric.close();
			}
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
				context.getCounter(Counters.COMBINE_INPUT_VALUES).increment(m.size());
				vec.add(m);
			}
			context.getCounter(Counters.COMBINE_OUTPUT_VALUES).increment(vec.size());
			context.write(col, vec);
		}
	}
	
	public static final class MCLReducer<M extends MCLMatrixSlice<M>> extends Reducer<SliceId, M, SliceId, M> {		
		
		private M vec = null;
		private final DistributedInt k_max = new DistributedIntMaximum();
		private final DistributedDouble chaos = new DistributedDoubleMaximum();
		private final MCLStats stats = new MCLStats();
		
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
				context.getCounter(Counters.REDUCE_INPUT_VALUES).increment(m.size());
				vec.add(m);
			}
			
			vec.inflateAndPrune(stats, context);
			k_max.set(stats.kmax);
			chaos.set(stats.maxChaos);
			context.getCounter(Counters.REDUCE_OUTPUT_VALUES).increment(vec.size());
			context.write(col, vec);
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			logger.debug("stats: {}",stats);
			ZkMetric.set(context.getConfiguration(), CHAOS, chaos);
			ZkMetric.set(context.getConfiguration(), KMAX, k_max);
			ZkMetric.close();
		}
	}
	
	@Override
	protected MCLResult run(List<Path> inputs, Path output) throws Exception {
		
		boolean computeChange = inputs.size() > 2;//TODO clean
		
		if(inputs == null || inputs.size() < 2 || output == null){
			throw new RuntimeException(String.format("invalid input/output: in=%s, out=%s",inputs,output));
		}
		//TODO calculate change
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
				computeChange 
				? CompositeInputFormat.compose("inner", SequenceFileInputFormat.class, inputs.get(0), inputs.get(1),inputs.get(2))
						: CompositeInputFormat.compose("inner", SequenceFileInputFormat.class, inputs.get(0), inputs.get(1)));
		
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
		ZkMetric.init(conf, CHAOS, true);
		ZkMetric.init(conf, KMAX, true);
		if(computeChange) ZkMetric.init(conf, SSD, true);
		
		MCLResult result = new MCLResult();
		result.run(job);
		
		meta.setKmax(ZkMetric.<DistributedInt>get(conf, KMAX).get());
		MatrixMeta.save(conf, output, meta);
		
		result.kmax = meta.getKmax();
		result.in_nnz = job.getCounters().findCounter(Counters.MAP_INPUT_VALUES).getValue();
		result.out_nnz = job.getCounters().findCounter(Counters.OUTPUT_NNZ).getValue();
		result.attractors = job.getCounters().findCounter(Counters.ATTRACTORS).getValue();
		result.homogenous_columns = job.getCounters().findCounter(Counters.HOMOGENEOUS_COLUMNS).getValue();
		result.cutoff = job.getCounters().findCounter(Counters.CUTOFF).getValue();
		result.prune = job.getCounters().findCounter(Counters.PRUNE).getValue();
		result.chaos = ZkMetric.<DistributedDouble>get(conf, CHAOS).get();
		result.changeInNorm = computeChange ? Math.sqrt(ZkMetric.<DistributedDouble>get(conf, SSD).get())/meta.getN() : Double.POSITIVE_INFINITY; 
		return result;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MCLStep(), args));
	}
}
