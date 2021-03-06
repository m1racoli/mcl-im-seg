package mapred.job;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
import mapred.params.Applyable;
import mapred.params.MCLAlgorithmParams;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
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

/**
 * the mclstep performs expansion, inflation and pruning in one job
 * 
 * @author Cedrik
 *
 */
public class MCLStep extends AbstractMCLJob {
	
	private static final Logger logger = LoggerFactory.getLogger(MCLStep.class);
	private static final String CHAOS = "/chaos";
	private static final String SSD = "/ssd";
	private static final String KMAX = "/kmax";	
	
	private final MCLAlgorithmParams algorithmParams = new MCLAlgorithmParams();
	
	private static final class MCLMapper<M extends MCLMatrixSlice<M>> extends Mapper<SliceId, TupleWritable, SliceId, M> {
		
		private final SliceId id = new SliceId();
		private DistributedDouble ssd = null; //sum squared differences
		private int last_id = -1;
		private long cpu_nanos = 0;
		private boolean local;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			cpu_nanos = 0;
			if(ssd != null) ssd.clear();
			local = MCLConfigHelper.getLocal(context.getConfiguration());
		}
		
		@SuppressWarnings("unchecked")
		@Override
		protected void map(SliceId key, TupleWritable tuple, Context context)
				throws IOException, InterruptedException {
//			logger.debug("MAP START");
			
			long start = System.nanoTime();
			
			M m = (M) tuple.get(0);
			
			if(last_id != key.get())
			{
				last_id = key.get();
				context.getCounter(Counters.MAP_INPUT_VALUES).increment(m.size());
				context.getCounter(Counters.MAP_INPUT_SLICES).increment(1);
				
				if(tuple.size() > 2)
				{					
					ssd = ssd == null ? new DistributedDoubleSum() : ssd;
					ssd.set(m.sumSquaredDifferences((M) tuple.get(2)));
				}
			}
			
			Writable mw = tuple.get(1);
			
			if(mw instanceof NullWritable){
				context.getCounter(Counters.NULL_BLOCKS).increment(1);
				return;
			}
			
			SubBlock<M> subBlock = (SubBlock<M>) mw;
			if(!subBlock.newData()){
				logger.debug("slice without blocks");
				context.getCounter(Counters.SLICE_WITHOUT_BLOCKS).increment(1);
				return;
			}
			
			id.set(subBlock.id);
			
			context.getCounter(Counters.MAP_INPUT_BLOCKS).increment(1);
			context.getCounter(Counters.MAP_INPUT_BLOCK_VALUES).increment(subBlock.subBlock.size());
			
			M product = subBlock.subBlock.multipliedBy(m);
			
			//count output records on diagonal and off diagonal
			if(id.get() == key.get()){
				context.getCounter(Counters.DIAG_MASS).increment(m.size());
			} else {
				context.getCounter(Counters.NON_DIAG_MASS).increment(m.size());
			}
			context.getCounter(Counters.MAP_OUTPUT_VALUES).increment(product.size());
			cpu_nanos += System.nanoTime() - start;
			//logger.debug("write slice with id={} and slice={}",id,product);
			
			context.write(id, product);
			//logger.debug("MAP END");
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			if(ssd != null){
				ZkMetric.set(context.getConfiguration(), SSD, ssd);
				if(!local) ZkMetric.close();
			}
			context.getCounter(Counters.MAP_CPU_MILLIS).increment(cpu_nanos/1000000L);
		}
	}
	
	private static final class MCLCombiner<M extends MCLMatrixSlice<M>> extends Reducer<SliceId, M, SliceId, M> {
		
		private M vec = null;
		private long cpu_nanos = 0;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			if(vec == null){
				vec = MCLContext.<M>getMatrixSliceInstance(context.getConfiguration());
			}
			cpu_nanos = 0;
		}
		
		@Override
		protected void reduce(SliceId col, Iterable<M> values, Context context)
				throws IOException, InterruptedException {
//			logger.debug("COMBINE START");
			
			long start; // = System.nanoTime();
			vec.clear();
			
			for(M m : values){
				start = System.nanoTime();
				context.getCounter(Counters.COMBINE_INPUT_VALUES).increment(m.size());
				vec.add(m);
				cpu_nanos += System.nanoTime() - start;
			}
			
			context.getCounter(Counters.COMBINE_OUTPUT_VALUES).increment(vec.size());
			context.write(col, vec);
			
//			logger.debug("COMBINE END");
		}
		
		@Override
		protected void cleanup(Reducer<SliceId, M, SliceId, M>.Context context)
				throws IOException, InterruptedException {
			context.getCounter(Counters.COMBINE_CPU_MILLIS).increment(cpu_nanos/1000000L);
		}
	}
	
	private static final class MCLReducer<M extends MCLMatrixSlice<M>> extends Reducer<SliceId, M, SliceId, M> {		
		
		private M vec = null;
		private final DistributedInt k_max = new DistributedIntMaximum();
		private final DistributedDouble chaos = new DistributedDoubleMaximum();
		private final MCLStats stats = new MCLStats();
		private long cpu_nanos = 0;
		private boolean local;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			if(vec == null){
				vec = MCLContext.<M>getMatrixSliceInstance(context.getConfiguration());
			}
			cpu_nanos = 0;
			stats.reset();
			local = MCLConfigHelper.getLocal(context.getConfiguration());
		}
		
		@Override
		protected void reduce(SliceId col, Iterable<M> values, Context context)
				throws IOException, InterruptedException {
//			logger.debug("REDUCE START");
			
			long start; // = System.nanoTime();
			vec.clear();
			for(M m : values){
				start = System.nanoTime();
				context.getCounter(Counters.REDUCE_INPUT_VALUES).increment(m.size());
				vec.add(m);
				cpu_nanos += System.nanoTime() - start;
			}
			start = System.nanoTime();
			vec.inflateAndPrune(stats);
			k_max.set(stats.kmax);
			chaos.set(stats.chaos);
			context.getCounter(Counters.REDUCE_OUTPUT_VALUES).increment(vec.size());
			cpu_nanos += System.nanoTime() - start;
			context.write(col, vec);
			
//			logger.debug("REDUCE END");
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			logger.debug("stats: {}",stats);
			ZkMetric.set(context.getConfiguration(), CHAOS, chaos);
			ZkMetric.set(context.getConfiguration(), KMAX, k_max);
			if(!local) ZkMetric.close();
			context.getCounter(Counters.REDUCE_CPU_MILLIS).increment(cpu_nanos/1000000L);
			context.getCounter(Counters.PRUNE).increment(stats.prune);
			context.getCounter(Counters.CUTOFF).increment(stats.cutoff);
			context.getCounter(Counters.ATTRACTORS).increment(stats.attractors);
			context.getCounter(Counters.HOMOGENEOUS_COLUMNS).increment(stats.homogen);
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
				? CompositeInputFormat.compose("outer", SequenceFileInputFormat.class, inputs.get(0), inputs.get(1),inputs.get(2))
						: CompositeInputFormat.compose("outer", SequenceFileInputFormat.class, inputs.get(0), inputs.get(1)));
		
		job.setMapperClass(MCLMapper.class);
		job.setMapOutputKeyClass(SliceId.class);
		job.setMapOutputValueClass(MCLConfigHelper.getMatrixSliceClass(conf));
		job.setOutputKeyClass(SliceId.class);
		job.setOutputValueClass(MCLConfigHelper.getMatrixSliceClass(conf));
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
		result.out_nnz = job.getCounters().findCounter(Counters.REDUCE_OUTPUT_VALUES).getValue();
		result.attractors = job.getCounters().findCounter(Counters.ATTRACTORS).getValue();
		result.homogenous_columns = job.getCounters().findCounter(Counters.HOMOGENEOUS_COLUMNS).getValue();
		result.cutoff = job.getCounters().findCounter(Counters.CUTOFF).getValue();
		result.prune = job.getCounters().findCounter(Counters.PRUNE).getValue();
		result.chaos = ZkMetric.<DistributedDouble>get(conf, CHAOS).get();
		result.changeInNorm = computeChange ? Math.sqrt(ZkMetric.<DistributedDouble>get(conf, SSD).get())/meta.getN() : Double.POSITIVE_INFINITY;
		return result;
	}
	
	@Override
	protected Collection<? extends Applyable> getParams() {
		return Collections.singletonList(algorithmParams);
	}
	
	@Override
	protected void setCommander(List<Object> list) {
		list.add(algorithmParams);
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MCLStep(), args));
	}
}
