/**
 * 
 */
package mapred.job.input;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import io.writables.Index;
import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceEntry;
import io.writables.SliceId;
import iterators.ReadOnlyIterator;
import mapred.Applyable;
import mapred.Counters;
import mapred.MCLConfigHelper;
import mapred.MCLContext;
import mapred.MCLInitParams;
import mapred.MCLResult;
import mapred.SlicePartitioner;
import mapred.job.AbstractMCLJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zookeeper.DistributedInt;
import zookeeper.DistributedIntMaximum;
import zookeeper.DistributedLong;
import zookeeper.DistributedLongMaximum;
import zookeeper.ZkMetric;

import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class InputAbcJob extends AbstractMCLJob {

	private static final Logger logger = LoggerFactory.getLogger(InputAbcJob.class);
	
	private static final String SCALE_CONF = "scale";
	private static final String KMAX = "/kmax";
	private static final String DIM = "/dim";
	
	@Parameter(names = {"-s","--scale"})
	private int scale = 1;
	
	private volatile MCLInitParams initParams = null;
	
	private static final class AnalyzeMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
		private final Pattern PATTERN = Pattern.compile("\t");
		private final DistributedLongMaximum n = new DistributedLongMaximum();
		private final LongWritable col = new LongWritable();
		private final IntWritable cnt = new IntWritable(1);
		private boolean local;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			local = MCLConfigHelper.getLocal(context.getConfiguration());
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			final String[] split = PATTERN.split(value.toString());
			final float v = Float.parseFloat(split[2]);
			
			if (v <= 0.0f) return;
			
			col.set(Long.parseLong(split[0]));
			n.set(col.get());
			n.set(Long.parseLong(split[1]));
			
			context.write(col, cnt);
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			ZkMetric.set(context.getConfiguration(), DIM, n);
			if(!local) ZkMetric.close();
		}
	}
	
	private static final class AnalyzeReducer extends Reducer<LongWritable, IntWritable, NullWritable, NullWritable> {
		
		private final DistributedIntMaximum kmax = new DistributedIntMaximum();
		private boolean local;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			local = MCLConfigHelper.getLocal(context.getConfiguration());
		}
		
		@Override
		protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int cnt = 0;
			
			for(IntWritable val : values){
				cnt += val.get();
			}
			
			kmax.set(cnt);			
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			ZkMetric.set(context.getConfiguration(), KMAX, kmax);
			if(!local) ZkMetric.close();
		}
	}
	
	private static final class AbcMapper extends Mapper<LongWritable, Text, Index, FloatWritable> {
		
		private final Pattern PATTERN = Pattern.compile("\t");
		private final Index idx = new Index();
		private final FloatWritable val = new FloatWritable();
		private int nsub;
		private int scale;
		private long n;
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Index, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			nsub = MCLConfigHelper.getNSub(context.getConfiguration());
			scale = context.getConfiguration().getInt(SCALE_CONF, 1);
			n = MCLConfigHelper.getN(context.getConfiguration());
		}
		
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			final String[] split = PATTERN.split(value.toString());
			final float v = Float.parseFloat(split[2]);
			
			if (v <= 0.0f) return;
						
			final long col = Long.parseLong(split[0]);
			final long row = Long.parseLong(split[1]);
			val.set(v);
			for(int i = scale-1; i >= 0; --i){
				long shift = i*n;				
				idx.set(col+shift, row+shift, nsub);
				context.write(idx, val);
			}
		}
	}
	
	private static final class AbcReducer<M extends MCLMatrixSlice<M>> extends Reducer<Index, FloatWritable, SliceId, M>{
		private M col = null;
		private final DistributedIntMaximum kmax = new DistributedIntMaximum();
		private boolean local;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			if(col == null){
				col = MCLContext.getMatrixSliceInstance(context.getConfiguration());
			}
			local = MCLConfigHelper.getLocal(context.getConfiguration());
		}
		
		@Override
		protected void reduce(final Index idx, final Iterable<FloatWritable> vals, Context context)
				throws IOException, InterruptedException {
			int kmax_tmp = col.fill(new Iterable<SliceEntry>() {
				
				@Override
				public Iterator<SliceEntry> iterator() {
					return new ValueIterator(idx, vals.iterator());
				}
				
			});
			col.addLoops(idx);
			col.makeStochastic();
			
			kmax.set(kmax_tmp);
			context.getCounter(Counters.MATRIX_SLICES).increment(1);
			context.getCounter(Counters.REDUCE_OUTPUT_VALUES).increment(col.size());
			context.write(idx.id, col);
		}
		
		@Override
		protected void cleanup(Reducer<Index, FloatWritable, SliceId, M>.Context context)
				throws IOException, InterruptedException {
			ZkMetric.set(context.getConfiguration(), KMAX, kmax);
			if(!local) ZkMetric.close();
		}
		
		private final class ValueIterator extends ReadOnlyIterator<SliceEntry> {

			private final Index idx;
			private final Iterator<FloatWritable> val;
			private final SliceEntry e = new SliceEntry();
			
			public ValueIterator(Index idx, Iterator<FloatWritable> val) {
				this.idx = idx;
				this.val = val;
			}
			
			@Override
			public boolean hasNext() {
				return val.hasNext();
			}

			@Override
			public SliceEntry next() {
				e.val = val.next().get();
				e.col = idx.col.get();
				e.row = idx.row.get();
				return e;
			}			
		}
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	protected MCLResult run(List<Path> inputs, Path output) throws Exception {
		
		if(inputs == null || inputs.size() == 0 || output == null) {
			throw new RuntimeException(String.format("invalid input/output: int=%s, out=%s", inputs,output));
		}
		
		final Path input = inputs.get(0);
		final Configuration conf = getConf();
		
		if(scale < 1){
			logger.error("scale={} must be a positve integer",scale);
			return null;
		}
		
		conf.setInt(SCALE_CONF, scale);
		
		// analyze job
		
		Job preJob = Job.getInstance(conf, "Analyse ABC Matrix: "+input);
		preJob.setJarByClass(getClass());
		
		preJob.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(preJob, input);
		
		preJob.setMapperClass(AnalyzeMapper.class);
		preJob.setMapOutputKeyClass(LongWritable.class);
		preJob.setMapOutputValueClass(IntWritable.class);
		preJob.setOutputKeyClass(NullWritable.class);
		preJob.setOutputValueClass(NullWritable.class);
		preJob.setCombinerClass(IntSumReducer.class);
		preJob.setReducerClass(AnalyzeReducer.class);
		preJob.setNumReduceTasks(MCLConfigHelper.getNumThreads(conf));
		preJob.setOutputFormatClass(NullOutputFormat.class);
		
		ZkMetric.init(conf, KMAX, true);
		ZkMetric.init(conf, DIM, true);
		
		MCLResult result = new MCLResult();
		result.run(preJob);
		
		if(!result.success) {
			logger.error("analysis job failed");
			return result;
		}
		
		// matrix meta
		
		int kmax = ZkMetric.<DistributedInt>get(conf, KMAX).get();
		long n = (ZkMetric.<DistributedLong>get(conf, DIM).get() + 1) * (long) scale;
		
		MatrixMeta meta = MatrixMeta.create(conf, n, kmax);
		
		// actual job
		
		Job job = Job.getInstance(conf, "Input to "+output.getName());
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, input);

		job.setMapperClass(AbcMapper.class);
		job.setMapOutputKeyClass(Index.class);
		job.setReducerClass(AbcReducer.class);

		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(SliceId.class);
		job.setOutputValueClass(MCLConfigHelper.getMatrixSliceClass(conf));
		job.setGroupingComparatorClass(IntWritable.Comparator.class);
		job.setNumReduceTasks(MCLConfigHelper.getNumThreads(conf));//TODO
		if(MCLConfigHelper.getNumThreads(conf) > 1) job.setPartitionerClass(SlicePartitioner.class);//TODO
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output);
		
		ZkMetric.init(conf, KMAX, true);
		
		result = new MCLResult();
		result.run(job);
		
		if(!result.success) return result;
		result.out_nnz = job.getCounters().findCounter(Counters.REDUCE_OUTPUT_VALUES).getValue();
		
		meta.setKmax(ZkMetric.<DistributedInt>get(conf, KMAX).get());
		result.kmax = meta.getKmax();
		result.n = n;
		
		MatrixMeta.save(conf, output, meta);		
		return result;
	}

	@Override
	protected Collection<? extends Applyable> getParams() {
		if (initParams == null) {
			initParams = new MCLInitParams();
		}
		
		return Arrays.asList(initParams);
	}
	
	@Override
	protected void setCommander(List<Object> list) {
		if (initParams == null) {
			initParams = new MCLInitParams();
		}
		
		list.add(initParams);
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new InputAbcJob(), args));
	}

}
