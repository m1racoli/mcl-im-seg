/**
 * 
 */
package mapred;

import java.io.IOException;

import io.writables.MCLMatrixSlice;
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
		private final SubBlock<M> subBlock = new SubBlock<M>();

		@Override
		protected void map(SliceId key, M value, Context context)
				throws IOException, InterruptedException {

			subBlock.id = key.get();
			for (M subBlock : value.getSubBlocks(id)) {
				this.subBlock.subBlock = subBlock;
				context.write(id, this.subBlock);
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

		return job.waitForCompletion(false);
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
