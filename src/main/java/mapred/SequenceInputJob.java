/**
 * 
 */
package mapred;

import java.awt.Point;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import io.writables.FeatureWritable;
import io.writables.Index;
import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceEntry;
import io.writables.SliceId;
import io.writables.SpatialFeatureWritable;
import io.writables.TOFPixel;
import iterators.ReadOnlyIterator;
import model.nb.RadialPixelNeighborhood;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class SequenceInputJob extends AbstractMCLJob {

	private static final Logger logger = LoggerFactory.getLogger(SequenceInputJob.class);
	
	private static final String NB_RADIUS_CONF = "nb.radius";
	private static final String DIM_WIDTH_CONF = "dim.width";
	private static final String DIM_HEIGHT_CONF = "dim.height";
	private static final String NUM_FRAMES_CONF = "num.frames";
	
	@Parameter(names = "-r")
	private float radius = 2.0f;
	
	@Parameter(names = "-w")
	private int w = 0;

	@Parameter(names = "-h")
	private int h = 0;
	
	@Parameter(names = "-f")
	private int f = 1;
	
	private volatile MCLInitParams initParams = null;
	
	private static final class SequenceInputMapper<V extends SpatialFeatureWritable<V>> extends Mapper<IntWritable, V, Index, V> {
		private final ArrayList<Point> list = new ArrayList<Point>();
		private int w = 0;
		private int h = 0;
		private int f = 1;
		private long l;
		private int nsub;
		private int r;
		private final Index idx1 = new Index();
		private final Index idx2 = new Index();
		private static RadialPixelNeighborhood nb = null;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			double radius = context.getConfiguration().getDouble(NB_RADIUS_CONF, 3.0);
			if(nb == null){
				nb = new RadialPixelNeighborhood(radius);
			}
			r = (int) radius;
			w = context.getConfiguration().getInt(DIM_WIDTH_CONF, w);
			h = context.getConfiguration().getInt(DIM_HEIGHT_CONF, h);
			l = (long) w * (long) h;
			f = context.getConfiguration().getInt(NUM_FRAMES_CONF, f);
			nsub = MCLConfigHelper.getNSub(context.getConfiguration());
		}
		
		private final long getIndex(long frame, Point p){
			return l * frame + (long) w * (long) p.y + (long) p.x;
		}
		
		private final long getIndex(Point p){
			return (long) w * (long) p.y + (long) p.x;
		}
		
		@Override
		protected void map(IntWritable key, V value, Context context)
				throws IOException, InterruptedException {
			final int frame = key.get();
			final long k1 = getIndex(frame, value.getPosition());
			
			idx1.id.set(MCLContext.getIdFromIndex(k1,nsub));
			idx1.col.set(MCLContext.getSubIndexFromIndex(k1,nsub));
			idx2.row.set(k1);
			
			for(Point p : nb.local(value.getPosition(), w, h, list)){
				final long k2_pre = getIndex(p);
				for(long i = Math.max(0, frame-r), end = Math.min(f-1, frame+r); i <= end; i++){
					final long k2 = l * i + k2_pre;
					idx1.row.set(k2);
					idx2.id.set(MCLContext.getIdFromIndex(k2,nsub));
					idx2.col.set(MCLContext.getSubIndexFromIndex(k2,nsub));
					
					context.write(idx1, value);
					context.write(idx2, value);
				}
				
			}
		}
	}
	
	private static final class InputReducer<M extends MCLMatrixSlice<M>,V extends FeatureWritable<V>> extends Reducer<Index, V, SliceId, M>{
		
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
		protected void reduce(final Index idx, final Iterable<V> pixels, Context context)
				throws IOException, InterruptedException {
			int kmax_tmp = col.fill(new Iterable<SliceEntry>() {
				
				@Override
				public Iterator<SliceEntry> iterator() {
					return new ValueIterator(idx, pixels.iterator());
				}
				
			});
			col.addLoops(idx);
			col.makeStochastic(context);
			
			if(kmax < kmax_tmp) kmax = kmax_tmp;
			context.getCounter(Counters.MATRIX_SLICES).increment(1);
			context.getCounter(Counters.NNZ).increment(col.size());
			context.write(idx.id, col);
		}
		
		@Override
		protected void cleanup(Reducer<Index, V, SliceId, M>.Context context)
				throws IOException, InterruptedException {
			MatrixMeta.writeKmax(context, kmax);
			//TODO multiple otput or correct path
		}
		
		private final class ValueIterator extends ReadOnlyIterator<SliceEntry> {

			private final SliceEntry entry = new SliceEntry();
			private final Index idx;
			private final Iterator<V> iter;
			
			private ValueIterator(Index idx, Iterator<V> iter){
				this.idx = idx;
				this.iter = iter;
			}
			
			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}
			
			@Override
			public SliceEntry next() {
				final V f1 = iter.next();
				
				entry.col = idx.col.get();
				entry.row = idx.row.get();
				entry.val = f1.dist(iter.next());
				return entry;
			}
		}		
	}
	
	private boolean loadMeta(Configuration conf, Path dir){
		
		try {
			FileSystem fs = FileSystem.get(conf);
			Path file = new Path(dir,"_meta");
			if(!fs.exists(file)){
				return false;
			}
			
			FSDataInputStream in = fs.open(file);
			w = in.readInt();
			h = in.readInt();
			f = in.readInt();			
			in.close();
			fs.close();
		} catch (IOException e) {
			logger.error(e.getMessage());
			return false;
		}
		
		return true;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	protected MCLResult run(List<Path> inputs, Path output) throws Exception {
		
		if(inputs == null || inputs.size() == 0 || output == null) {
			throw new RuntimeException(String.format("invalid input/output: int=%s, out=%s", inputs,output));
		}
		
		Path input = inputs.get(0);
		
		if(!loadMeta(getConf(), input)){
			if(w == 0 || h == 0){
				logger.error("dimensions (weigth,height) not set");
				return null;
			}
			
			logger.warn("no meta information found");
		}
		
		final Configuration conf = getConf();
		conf.setFloat(NB_RADIUS_CONF, radius);
		conf.setInt(DIM_HEIGHT_CONF, h);
		conf.setInt(DIM_WIDTH_CONF, w);
		conf.setInt(NUM_FRAMES_CONF, f);
		
		int kmax = RadialPixelNeighborhood.size(radius) * (2*(int) radius + 1);
		long n = (long) w * (long) h * (long) f;		
		
		MatrixMeta meta = MatrixMeta.create(conf, n, kmax);
		
		Job job = Job.getInstance(conf, "SequenceInputJob");
		job.setJarByClass(SequenceInputJob.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, input);		

		job.setMapperClass(SequenceInputMapper.class);
		job.setMapOutputKeyClass(Index.class);
		job.setMapOutputValueClass(TOFPixel.class); //TODO dynamic		
		job.setOutputKeyClass(SliceId.class);
		job.setOutputValueClass(MCLConfigHelper.getMatrixSliceClass(conf));
		job.setReducerClass(InputReducer.class);
		job.setGroupingComparatorClass(IntWritable.Comparator.class);
		job.setNumReduceTasks(MCLConfigHelper.getNumThreads(conf));//TODO
		if(MCLConfigHelper.getNumThreads(conf) > 1) job.setPartitionerClass(SlicePartitioner.class);//TODO
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output);
		
		MCLResult result = new MCLResult();
		result.run(job);
		
		if(!result.success) return result;
		result.nnz = job.getCounters().findCounter(Counters.NNZ).getValue();
		
		meta.mergeKmax(conf, output);
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
		System.exit(ToolRunner.run(new SequenceInputJob(), args));
	}

}
