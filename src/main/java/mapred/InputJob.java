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
import io.writables.Pixel;
import io.writables.SliceId;
import model.nb.RadialPixelNeighborhood;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.ReadOnlyIterator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class InputJob extends Configured implements Tool {

	private static final Logger logger = LoggerFactory.getLogger(InputJob.class);
	private static final String NB_RADIUS_CONF = "nb.radius";
	
	@Parameter(names = "-i")
	private String input = null;
	
	@Parameter(names = "-o")
	private String output = null;
	
	@Parameter(names = "-r")
	private Float radius = 3.0f;
	
	private static final class InputMapper extends Mapper<LongWritable, Pixel, Index, Pixel> {
		private final ArrayList<Point> list = new ArrayList<Point>();
		private final int w = 480;
		private final int h = 300;
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
			col.clear();
			col.add(idx.col, idx.row, new Iterable<Float>() {
				
				@Override
				public Iterator<Float> iterator() {
					return new ValueIterator(idx, pixels.iterator());
				}
			});

			context.getCounter(Counters.NNZ).increment(col.size());
			context.write(idx.id, col);
		}
		
		private final class ValueIterator extends ReadOnlyIterator<Float> {

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
			public Float next() {
				final V f1 = iter.next();
				
				if(idx.isDiagonal()){
					return 1.0f;
				}
				
				return f1.dist(iter.next()); //TODO dist
			}

		}
		
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {

		final Configuration conf = getConf();
		conf.setFloat(NB_RADIUS_CONF, radius);
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
		job.setGroupingComparatorClass(LongWritable.Comparator.class);
		job.setNumReduceTasks(MCLContext.getNumThreads());
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		InputJob job = new InputJob();
		JCommander cmd = new JCommander(job);
		cmd.addObject(MCLContext.instance);
		cmd.parse(args);
		
		int rc = ToolRunner.run(job, args);
		System.exit(rc);
	}

}
