/**
 * 
 */
package mapred;

import java.io.IOException;
import java.util.List;

import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceId;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Cedrik
 *
 */
public class NativeInputJob extends AbstractMCLJob {

	private final static class DirectReducer<M extends MCLMatrixSlice<M>> extends Reducer<SliceId, M, SliceId, M> {
		
		@Override
		protected void reduce(SliceId id, Iterable<M> values, Context context)
				throws IOException, InterruptedException {
			for(M m : values){
				context.getCounter(Counters.MATRIX_SLICES).increment(1);
				context.getCounter(Counters.NNZ).increment(m.size());
				context.write(id, m);
			}
		}
	}
	
	@Override
	protected MCLResult run(List<Path> inputs, Path output)
			throws Exception {
		
		Configuration conf = getConf();
		Path input = inputs.get(0);
		
		MatrixMeta meta = MatrixMeta.load(conf, input);
		meta.apply(getConf());
		long n = meta.getN();
		
		Job job = Job.getInstance(conf, "DirectInputJob");
		job.setJarByClass(NativeInputJob.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, input);
		
		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(SliceId.class);
		job.setMapOutputValueClass(MCLConfigHelper.getMatrixSliceClass(conf));
		job.setOutputKeyClass(SliceId.class);
		job.setOutputValueClass(MCLConfigHelper.getMatrixSliceClass(conf));
		job.setReducerClass(DirectReducer.class);
		job.setNumReduceTasks(MCLConfigHelper.getNumThreads(conf));//TODO
		if(MCLConfigHelper.getNumThreads(conf) > 1) job.setPartitionerClass(SlicePartitioner.class);//TODO
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output);

		MCLResult result = new MCLResult();
		result.run(job);
		
		if(!result.success) return result;
		result.nnz = job.getCounters().findCounter(Counters.NNZ).getValue();
		
		result.kmax = meta.getKmax();
		result.n = n;
		
		MatrixMeta.save(conf, output, meta);	
		return result;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new NativeInputJob(), args));
	}

}
