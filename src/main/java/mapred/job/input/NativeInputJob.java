/**
 * 
 */
package mapred.job.input;

import java.util.List;

import io.writables.MatrixMeta;
import mapred.MCLResult;
import mapred.job.AbstractMCLJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 * this just copies data to the target folder
 * 
 * @author Cedrik
 *
 */
public class NativeInputJob extends AbstractMCLJob {

//	private final static class DirectReducer<M extends MCLMatrixSlice<M>> extends Reducer<SliceId, M, SliceId, M> {
//		
//		@Override
//		protected void reduce(SliceId id, Iterable<M> values, Context context)
//				throws IOException, InterruptedException {
//			for(M m : values){
//				context.getCounter(Counters.MATRIX_SLICES).increment(1);
//				context.getCounter(Counters.NNZ).increment(m.size());
//				context.write(id, m);
//			}
//		}
//	}
	
	@Override
	protected MCLResult run(List<Path> inputs, Path output)
			throws Exception {
		
		Configuration conf = getConf();
		Path input = inputs.get(0);
		
		MatrixMeta meta = MatrixMeta.load(conf, input);
		meta.apply(getConf());
		
		FileSystem infs = input.getFileSystem(conf);
		FileSystem outFs = FileSystem.get(conf);
		FileUtil.copy(infs, input, outFs, output, false, conf);
		
//		long n = meta.getN();
//		
//		Job job = Job.getInstance(conf, "DirectInputJob");
//		job.setJarByClass(NativeInputJob.class);
//
//		job.setInputFormatClass(SequenceFileInputFormat.class);
//		SequenceFileInputFormat.setInputPaths(job, input);
//		
//		job.setMapperClass(Mapper.class);
//		job.setMapOutputKeyClass(SliceId.class);
//		job.setMapOutputValueClass(MCLConfigHelper.getMatrixSliceClass(conf));
//		job.setOutputKeyClass(SliceId.class);
//		job.setOutputValueClass(MCLConfigHelper.getMatrixSliceClass(conf));
//		job.setReducerClass(DirectReducer.class);
//		job.setNumReduceTasks(MCLConfigHelper.getNumThreads(conf));//TODO
//		if(MCLConfigHelper.getNumThreads(conf) > 1) job.setPartitionerClass(SlicePartitioner.class);//TODO
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		SequenceFileOutputFormat.setOutputPath(job, output);
//
//		MCLResult result = new MCLResult();
//		result.run(job);
//		
//		if(!result.success) return result;
//		result.nnz = job.getCounters().findCounter(Counters.NNZ).getValue();
//		
//		result.kmax = meta.getKmax();
//		result.n = n;
//		
//		MatrixMeta.save(conf, output, meta);	
		
		MCLResult result = new MCLResult();
		result.success = true;
		result.n = meta.getN();
		result.kmax = meta.getKmax();
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
