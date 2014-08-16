/**
 * 
 */
package mapred;

import java.io.IOException;

import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceId;
import io.writables.SubBlock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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

	private static final class TransposeMapper<M extends MCLMatrixSlice<M>> extends
			Mapper<SliceId, M, SliceId, SubBlock<M>> {

		private final SliceId id = new SliceId();
		private SubBlock<M> subBlock = null;
		
		@Override
		protected void setup(
				Mapper<SliceId, M, SliceId, SubBlock<M>>.Context context)
				throws IOException, InterruptedException {
			MCLContext.get(context.getConfiguration());
			
			if(subBlock == null) {
				subBlock = new SubBlock<M>();
			}
		}

		@Override
		protected void map(SliceId key, M value, Context context)
				throws IOException, InterruptedException {

			subBlock.id = key.get();
			for (M m : value.getSubBlocks(id)) {
				subBlock.subBlock = m;
				context.write(id, subBlock);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {

		final Path inPath = new Path(input);
		final Path outPath = new Path(output);

		return run(getConf(), inPath, outPath) ? 0 : 1;
	}

	public static boolean run(Configuration conf, Path input, Path output)
			throws Exception {
		
		Logger.getRootLogger().setLevel(Level.WARN);
		Logger.getLogger(Job.class).setLevel(Level.INFO); //TODO progress
		
		Logger.getLogger("mapred").setLevel(Level.DEBUG);
		Logger.getLogger("io.writables").setLevel(Level.DEBUG);
		
		MatrixMeta meta = MatrixMeta.load(conf, input);
		MCLContext.setKMax(meta.k_max);
		MCLContext.set(conf);
		
		if (output.getFileSystem(conf).exists(output)) {
			output.getFileSystem(conf).delete(output, true);
		}

		Job job = Job.getInstance(conf, "TransposeJob " + input.getName() + " "
				+ output.getName());
		job.setJarByClass(TransposeJob.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, input);

		job.setMapperClass(TransposeMapper.class);
		job.setMapOutputKeyClass(SliceId.class);
		job.setMapOutputValueClass(SubBlock.class);
		job.setOutputKeyClass(SliceId.class);
		job.setOutputValueClass(SubBlock.class);
		job.setNumReduceTasks(MCLContext.getNumThreads());

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true);
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
