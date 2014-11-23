/**
 * 
 */
package classic;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

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
		
		private final SortedMap<Integer,SortedMap<Integer,M>> map = new TreeMap<Integer, SortedMap<Integer,M>>();
		
		private void run(Path input, Path output) throws IOException {
			
			{
				Reader reader = new Reader(getConf(), Reader.file(input));			
				
				SliceId key = new SliceId();
				M m = MCLContext.getMatrixSliceInstance(getConf());
				SliceId id = new SliceId();
				
				while(reader.next(key, m)){
					for(M s : m.getSubBlocks(id)){
						add(id.get(), key.get(), s.deepCopy());
					}
				}
				
				reader.close();
			}
			
			Writer writer = SequenceFile.createWriter(getConf(), 
					Writer.file(output), 
					Writer.keyClass(SliceId.class),
					Writer.valueClass(SubBlock.class));
			
			SubBlock<M> subBlock = new SubBlock<M>();
			subBlock.setConf(getConf(), false);
			SliceId id = new SliceId();
			
			for(Entry<Integer,SortedMap<Integer,M>> e1 : map.entrySet()){
				id.set(e1.getKey());
				SortedMap<Integer, M> subMap = e1.getValue();
				for(Entry<Integer,M> e2 : subMap.entrySet()){
					subBlock.id = e2.getKey();
					subBlock.subBlock = e2.getValue();
					writer.append(id, subBlock);
				}
			}
			
			writer.close();
			map.clear();
		}
		
		private void add(Integer id, Integer block_id, M m){
			SortedMap<Integer,M> subMap = map.get(id);
			
			if(subMap == null){
				subMap = new TreeMap<Integer, M>();
				map.put(id, subMap);
			}
			subMap.put(block_id, m);
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
