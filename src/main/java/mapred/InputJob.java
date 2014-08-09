/**
 * 
 */
package mapred;

import java.awt.Point;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import io.writables.Feature;
import io.writables.FeatureWritable;
import io.writables.Index;
import io.writables.MCLMatrixSlice;
import io.writables.Pixel;
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
			if(nb == null){
				synchronized (InputMapper.class) {
					if(nb == null){
						nb = new RadialPixelNeighborhood(context.getConfiguration().getFloat(NB_RADIUS_CONF, 3));
					}
				}
			}
			idx2.col = idx1.row;
		}
		
		@Override
		protected void map(LongWritable key, Pixel value, Context context)
				throws IOException, InterruptedException {
			idx1.col = key;
			idx2.row = key;
			
			for(Point p : nb.local(value.x, value.y, w, h, list)){
				idx1.row.set((long) p.x + (long) w * (long) p.y);

				context.write(idx1, value);
				
				if(idx1.isDiagonal())
					continue;
				
				context.write(idx2, value);
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	private static final class InputReducer extends Reducer<Index, Feature, LongWritable, MCLMatrixSlice>{
		
		private MCLMatrixSlice col = null;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			MCLContext.get(context);
			if(col == null){
				col = MCLContext.getMatrixSliceInstance(context.getConfiguration());
			}
			
		}
		
		@SuppressWarnings("unchecked")
		@Override
		protected void reduce(final Index idx, final Iterable<Feature> pixels, Context context)
				throws IOException, InterruptedException {
			col.construct(idx.row, new Iterable<Float>() {
				
				@Override
				public Iterator<Float> iterator() {
					return new ValueIterator(idx, pixels.iterator());
				}
			});
			col.init(idx, pixels);
			context.getCounter(Counters.NON_NULL_VALUES).increment(col.size());
			context.write(idx.col, col);
		}
		
		private static final class ValueIterator<V extends Feature<V>> extends ReadOnlyIterator<Float> {

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
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(MCLContext.getMatrixSliceClass());
		job.setGroupingComparatorClass(LongWritable.Comparator.class);
		job.setNumReduceTasks(2);
		
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
		new JCommander(job,args);
		int rc = ToolRunner.run(job, args);
		System.exit(rc);
	}

}
