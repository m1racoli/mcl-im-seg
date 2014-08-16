/**
 * 
 */
package mapred;

import java.awt.Point;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import io.writables.FeatureWritable;
import io.writables.Index;
import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.Pixel;
import io.writables.SliceId;
import io.writables.MCLMatrixSlice.MatrixEntry;
import model.nb.RadialPixelNeighborhood;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import util.ReadOnlyIterator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class InputJob extends Configured implements Tool {

	private static final Logger logger = Logger.getLogger(InputJob.class);
	private static final String NB_RADIUS_CONF = "nb.radius";
	private static final String DIM_WIDTH_CONF = "dim.width";
	private static final String DIM_HEIGHT_CONF = "dim.height";
	
	@Parameter(names = "-i")
	private String input = null;
	
	@Parameter(names = "-o")
	private String output = null;
	
	@Parameter(names = "-r")
	private Float radius = 3.0f;
	
	@Parameter(names = "-w")
	private int w = 480;

	@Parameter(names = "-h")
	private int h = 300;
	
	@Parameter(names = "-debug")
	private boolean debug = false;
	
	@Parameter(names = "-profile")
	private boolean profile = false;
	
	private static final class InputMapper extends Mapper<LongWritable, Pixel, Index, Pixel> {
		private final ArrayList<Point> list = new ArrayList<Point>();
		private int w = 480;
		private int h = 300;
		private final Index idx1 = new Index();
		private final Index idx2 = new Index();
		private static RadialPixelNeighborhood nb = null;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			MCLContext.get(context.getConfiguration());
			if(nb == null){
				synchronized (InputMapper.class) {
					if(nb == null){
						nb = new RadialPixelNeighborhood(context.getConfiguration().getFloat(NB_RADIUS_CONF, 3));
					}
				}
			}
			w = context.getConfiguration().getInt(DIM_WIDTH_CONF, w);
			h = context.getConfiguration().getInt(DIM_HEIGHT_CONF, h);
		}
		
		@Override
		protected void map(LongWritable key, Pixel value, Context context)
				throws IOException, InterruptedException {
			final long k1 = key.get();
			idx1.id.set(MCLContext.getIdFromIndex(k1));
			idx1.col.set(MCLContext.getSubIndexFromIndex(k1));
			idx2.row.set(k1);
			
			for(Point p : nb.local(value.x, value.y, w, h, list)){
				final long k2 = (long) p.x + (long) w * (long) p.y;
				idx1.row.set(k2);
				idx2.id.set(MCLContext.getIdFromIndex(k2));
				idx2.col.set(MCLContext.getSubIndexFromIndex(k2));

				context.write(idx1, value);
				
				if(idx1.isDiagonal())
					continue;
				
				context.write(idx2, value);
			}
		}
	}
	
	private static final class InputReducer<M extends MCLMatrixSlice<M>,V extends FeatureWritable<V>> extends Reducer<Index, V, SliceId, M>{
		
		private M col = null;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			MCLContext.get(context);
			if(col == null){
				col = MCLContext.getMatrixSliceInstance(context.getConfiguration());
			}
			
		}
		
		@Override
		protected void reduce(final Index idx, final Iterable<V> pixels, Context context)
				throws IOException, InterruptedException {
			col.fill(new Iterable<MatrixEntry>() {
				
				@Override
				public Iterator<MatrixEntry> iterator() {
					return new ValueIterator(idx, pixels.iterator());
				}
				
			});
			
			context.getCounter(Counters.MATRIX_SLICES).increment(1);
			context.getCounter(Counters.NNZ).increment(col.size());
			context.write(idx.id, col);
		}
		
		private final class ValueIterator extends ReadOnlyIterator<MatrixEntry> {

			private final MatrixEntry entry = new MatrixEntry();
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
			public MatrixEntry next() {
				final V f1 = iter.next();
				
				entry.col = idx.col.get();
				entry.row = idx.row.get();
				
				if(idx.isDiagonal()){
					entry.val = 1.0f;
					return entry;
				}
				
				entry.val = f1.dist(iter.next()); //TODO dist
				return entry;
			}

		}
		
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {

		Logger.getRootLogger().setLevel(Level.WARN);
		Logger.getLogger(Job.class).setLevel(Level.INFO); //TODO progress
		
		if (debug) {
			Logger.getLogger("mapred").setLevel(Level.DEBUG);
			Logger.getLogger("io.writables").setLevel(Level.DEBUG);
		}
		
		final Configuration conf = getConf();
		conf.setFloat(NB_RADIUS_CONF, radius);
		conf.setInt(DIM_HEIGHT_CONF, h);
		conf.setInt(DIM_WIDTH_CONF, w);
		
//		if(profile) {
//			conf.setBoolean("mapreduce.task.profile", profile);
//			conf.set("mapreduce.task.profile.params", "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=n,verbose=n,file=%s");
//			conf.setInt("mapreduce.map.memory.mb",1024);
//			conf.setInt("mapreduce.reduce.memory.mb",1024);
//		}
		
		int k_max = RadialPixelNeighborhood.size(radius);
		long n = (long) w * (long) h;
		
		MatrixMeta meta = MatrixMeta.create(n, MCLContext.getNSub(), k_max);		
		MCLContext.setKMax(k_max);
		MCLContext.setN(n);
		MCLContext.set(conf);
		
		final Path input = new Path(this.input);
		final Path output = new Path(this.output);
		
		if(output.getFileSystem(conf).exists(output)){
			output.getFileSystem(conf).delete(output, true);
		}
		
		Job job = Job.getInstance(conf, "Input to "+output.getName());
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, input);		

		job.setMapperClass(InputMapper.class);
		job.setMapOutputKeyClass(Index.class);
		job.setReducerClass(InputReducer.class);

		job.setMapOutputValueClass(Pixel.class);
		job.setOutputKeyClass(SliceId.class);
		job.setOutputValueClass(MCLContext.getMatrixSliceClass());
		job.setGroupingComparatorClass(IntWritable.Comparator.class);
		job.setNumReduceTasks(MCLContext.getNumThreads());
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output);
		
		int rc = job.waitForCompletion(debug) ? 0 : 1;
		
		if(rc != 0) return rc;
		
		MatrixMeta.save(conf, output, meta);
		
		return rc;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		InputJob job = new InputJob();
		JCommander cmd = new JCommander(job);
		cmd.setAcceptUnknownOptions(true);
		cmd.addObject(MCLContext.instance);
		cmd.parse(args);
		
		int rc = ToolRunner.run(job, args);
		System.exit(rc);
	}

}
