/**
 * 
 */
package classic;

import java.io.IOException;
import java.util.List;

import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceId;
import io.writables.SubBlock;
import mapred.MCLContext;
import mapred.MCLResult;
import mapred.job.AbstractMCLJob;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Cedrik
 *
 */
public class InMemoryTransposeJob extends AbstractMCLJob {

	private final class TransposeRunner<M extends MCLMatrixSlice<M>> {
		
		private void run(Path input, Path output) throws IOException {
			Reader reader = new Reader(getConf(), Reader.file(input));
			Writer writer = SequenceFile.createWriter(getConf(), 
					Writer.file(output), 
					Writer.keyClass(SliceId.class),
					Writer.valueClass(SubBlock.class));
			
			SliceId key = new SliceId();
			M m = MCLContext.getMatrixSliceInstance(getConf());
			SliceId id = new SliceId();
			SubBlock<M> subBlock = new SubBlock<M>();
			subBlock.setConf(getConf(), false);
			
			while(reader.next(key, m)){
				subBlock.id = key.get();
				for(M s : m.getSubBlocks(id)){
					subBlock.subBlock = s;
					writer.append(id, subBlock);
				}
			}
			
			reader.close();
			writer.close();
		}
	}

	@Override
	protected MCLResult run(List<Path> inputs, Path output)
			throws Exception {
		
		Path input = inputs.get(0);
		
		MatrixMeta meta = MatrixMeta.load(getConf(), input);
		meta.apply(getConf());
		
		@SuppressWarnings("rawtypes")
		TransposeRunner runner = new TransposeRunner();
		
		FileSystem fs = input.getFileSystem(getConf());
		RemoteIterator<LocatedFileStatus> it = fs.listFiles(input, false);
		
		while(it.hasNext()){
			LocatedFileStatus status = it.next();
			String filename = status.getPath().getName();
			if(filename.startsWith(".") || filename.startsWith("_"))
				continue;
			runner.run(status.getPath(), new Path(output, filename));
		}

		MCLResult result = new MCLResult();
		result.success = true;
		result.counters = new Counters();
		
		MatrixMeta.save(getConf(), output, meta);
		return result;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new InMemoryTransposeJob(), args));
	}

}
