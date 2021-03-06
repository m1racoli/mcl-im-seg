/**
 * 
 */
package mapred.job;

import java.io.IOException;
import java.util.List;

import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceId;
import io.writables.SubBlock;
import mapred.Counters;
import mapred.MCLConfigHelper;
import mapred.MCLResult;
import mapred.SlicePartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * the transpose job generates a transposed block matrix from an input slice matrix
 * 
 * @author Cedrik
 *
 */
public class TransposeJob extends AbstractMCLJob {
	
	private static final class TransposeMapper<M extends MCLMatrixSlice<M>> extends
			Mapper<SliceId, M, SliceId, SubBlock<M>> {

		private final SliceId id = new SliceId();
		private SubBlock<M> subBlock = new SubBlock<M>();
		private long cpu_nanos = 0;
		
		@Override
		protected void setup(
				Mapper<SliceId, M, SliceId, SubBlock<M>>.Context context)
				throws IOException, InterruptedException {
			subBlock.setConf(context.getConfiguration(), false);
			cpu_nanos = 0;
		}
		
		@Override
		protected void map(SliceId key, M value, Context context)
				throws IOException, InterruptedException {
			long start = System.nanoTime();
			subBlock.id = key.get();
			for (M m : value.getSubBlocks(id)) {
				subBlock.subBlock = m;
				context.getCounter(Counters.MAP_OUTPUT_BLOCKS).increment(1);
				context.getCounter(Counters.MAP_OUTPUT_VALUES).increment(m.size());
				cpu_nanos += System.nanoTime() - start;
				context.write(id, subBlock);
				start = System.nanoTime();
			}
			cpu_nanos += System.nanoTime() - start;
		}
		
		@Override
		protected void cleanup(
				Mapper<SliceId, M, SliceId, SubBlock<M>>.Context context)
				throws IOException, InterruptedException {
			context.getCounter(Counters.MAP_CPU_MILLIS).increment(cpu_nanos/1000000L);
		}
	}

	@Override
	protected MCLResult run(List<Path> inputs, Path output)
			throws Exception {
		
		Configuration conf = getConf();
		Path input = inputs.get(0);
		
		MatrixMeta meta = MatrixMeta.load(conf, input);
		meta.apply(getConf());

		Job job = Job.getInstance(conf, "TransposeJob");
		job.setJarByClass(TransposeJob.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, input);

		job.setMapperClass(TransposeMapper.class);
		job.setMapOutputKeyClass(SliceId.class);
		job.setMapOutputValueClass(SubBlock.class);
		job.setOutputKeyClass(SliceId.class);
		job.setOutputValueClass(SubBlock.class);
		job.setNumReduceTasks(MCLConfigHelper.getNumThreads(conf));//TODO
		if(MCLConfigHelper.getNumThreads(conf) > 1) job.setPartitionerClass(SlicePartitioner.class);//TODO
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output);

		MCLResult result = new MCLResult();
		result.run(job);
		
		MatrixMeta.save(conf, output, meta);
		return result;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new TransposeJob(), args));
	}

}
