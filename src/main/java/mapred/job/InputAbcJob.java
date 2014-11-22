/**
 * 
 */
package mapred.job;

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
import model.nb.RadialPixelNeighborhood;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class InputAbcJob extends AbstractMCLJob {

	private static final Logger logger = LoggerFactory.getLogger(InputAbcJob.class);
	
	private static final String NB_RADIUS_CONF = "nb.radius";
	private static final String DIM_WIDTH_CONF = "dim.width";
	private static final String DIM_HEIGHT_CONF = "dim.height";
	
	@Parameter(names = "-r")
	private Float radius = 3.0f;
	
	@Parameter(names = "-w")
	private int w = 480;

	@Parameter(names = "-h")
	private int h = 300;
	
	private Applyable initParams = null;
	
	private static final class AbcMapper extends Mapper<LongWritable, Text, Index, FloatWritable> {
		
		private final Pattern PATTERN = Pattern.compile("\t");
		private final Index idx = new Index();
		private final FloatWritable val = new FloatWritable();
		private int nsub;
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Index, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			nsub = MCLConfigHelper.getNSub(context.getConfiguration());
		}
		
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			final String[] split = PATTERN.split(value.toString());
			final float v = Float.parseFloat(split[2]);
			
			if (v <= 0.0f) return;
						
			final long col = Long.parseLong(split[0]);
			final long row = Long.parseLong(split[1]);
			val.set(v);
			idx.set(col, row, nsub);
			context.write(idx, val);
			//idx.set(row, col, nsub);
			//context.write(idx, val);
		}
	}
	
	private static final class AbcReducer<M extends MCLMatrixSlice<M>> extends Reducer<Index, FloatWritable, SliceId, M>{
		private M col = null;
		private int kmax = 0;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			if(col == null){
				col = MCLContext.getMatrixSliceInstance(context.getConfiguration());
			}
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
			col.makeStochastic(context);
			
			if(kmax < kmax_tmp) kmax = kmax_tmp;
			context.getCounter(Counters.MATRIX_SLICES).increment(1);
			context.getCounter(Counters.REDUCE_OUTPUT_VALUES).increment(col.size());
			context.write(idx.id, col);
		}
		
		@Override
		protected void cleanup(Reducer<Index, FloatWritable, SliceId, M>.Context context)
				throws IOException, InterruptedException {
			MatrixMeta.writeKmax(context, kmax);
			//TODO multiple otput or correct path
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
		
		final Configuration conf = getConf();
		conf.setFloat(NB_RADIUS_CONF, radius);
		conf.setInt(DIM_HEIGHT_CONF, h);
		conf.setInt(DIM_WIDTH_CONF, w);
		
		int kmax = RadialPixelNeighborhood.size(radius);
		long n = (long) w * (long) h;		
		
		MatrixMeta meta = MatrixMeta.create(conf, n, kmax);
		
		Job job = Job.getInstance(conf, "Input to "+output.getName());
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, inputs.get(0));

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
		
		MCLResult result = new MCLResult();
		result.run(job);
		
		if(!result.success) return result;
		result.out_nnz = job.getCounters().findCounter(Counters.REDUCE_OUTPUT_VALUES).getValue();
		
//		while(job.cleanupProgress() < 1) {
//			logger.debug("wait for cleanup");
//			Thread.sleep(200);
//		}
		
		meta.mergeKmax(conf, output);
		result.kmax = meta.getKmax();
		result.n = n;
		
		MatrixMeta.save(conf, output, meta);		
		return result;
	}

	@Override
	protected Collection<Applyable> getParams() {
		if (initParams == null) {
			initParams = new MCLInitParams();
		}
		
		return Arrays.asList(initParams);
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new InputAbcJob(), args));
	}

}
