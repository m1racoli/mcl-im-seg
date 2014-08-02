/**
 * 
 */
package mapred;

import java.io.IOException;

import io.writables.Column;
import io.writables.Index;
import io.writables.MCLMatrixSlice;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class TransposeJob extends Configured implements Tool {

	@Parameter(names = "-i")
	private String input = null;
	
	@Parameter(names = "-o")
	private String output = null;
	
	private static final class TransposeMapper extends Mapper<LongWritable, MCLMatrixSlice<?,? extends Writable>, Index, Writable> {
		
		private final Index idx = new Index();
		
		@Override
		protected void map(LongWritable key, MCLMatrixSlice<?,? extends Writable> value, Context context)
				throws IOException, InterruptedException {
			
			idx.row.set(key.get());
			for(Writable subBlock : value.subBlocks(idx.col)){
				context.write(idx, subBlock);//TODO kcount per block
			}
			
		}
	}
	
	@SuppressWarnings("rawtypes")
	private static final class TransposeReducer extends Reducer<Index, Writable, LongWritable, MCLMatrixSlice> {
		
		private MCLMatrixSlice slice;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			MCLContext.get(context);
			if(slice == null){
				slice = MCLContext.getMatrixSliceInstance();
			}
		}
		
		@SuppressWarnings("unchecked")
		@Override
		protected void reduce(Index idx, Iterable<Writable> values, Context context)
				throws IOException, InterruptedException {

			slice.combine(idx.row, values);
			context.write(idx.col, slice);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		
		final Path inPath = new Path(input);
		final Path outPath = new Path(output);
		
		if(outPath.getFileSystem(getConf()).exists(outPath)){
			outPath.getFileSystem(getConf()).delete(outPath, true);
		}
		
		Job job = Job.getInstance(getConf(),"TransposeJob "+inPath.getName()+" "+outPath.getName());
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inPath);
		
		job.setMapperClass(TransposeMapper.class);
		job.setMapOutputKeyClass(Index.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Column.class);
		job.setReducerClass(TransposeReducer.class);
		job.setGroupingComparatorClass(LongWritable.Comparator.class);
		job.setNumReduceTasks(2);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outPath);		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		TransposeJob job = new TransposeJob();
		new JCommander(job, args);
		System.exit(ToolRunner.run(job, args));
	}

}
