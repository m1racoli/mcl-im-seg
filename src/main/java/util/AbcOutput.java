/**
 * 
 */
package util;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceEntry;
import io.writables.SliceId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * transforms a distributed slice matrix to abc format
 * 
 * @author Cedrik
 *
 */
public class AbcOutput extends AbstractUtil {

	private static final Logger logger = LoggerFactory.getLogger(AbcOutput.class);
	
	/* (non-Javadoc)
	 * @see util.AbstractUtil#run(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, boolean)
	 */
	@Override
	protected int run(Path input, Path output, boolean hdfsOutput)
			throws Exception {
		
		Configuration conf = getConf();
		
		MatrixMeta meta = MatrixMeta.load(conf, input);
		
		if(meta == null){
			return 1;
		}
		
		FileSystem inFs = FileSystem.get(conf);
		FileSystem outfs = hdfsOutput ? FileSystem.get(conf) : FileSystem.getLocal(conf);		
		
		logger.info(meta.toString());
		
		meta.apply(conf);
		final long nsub = meta.getNSub();
		final SliceId id = new SliceId();
		MCLMatrixSlice<?> m = null;
		long nnz = 0;
		int num_files = 0;
		
		RemoteIterator<LocatedFileStatus> it = inFs.listFiles(input, false);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outfs.create(output, true)));
		
		while(it.hasNext()){
			LocatedFileStatus fileStatus = it.next();
			Path file = fileStatus.getPath();
			String fileName = file.getName();
			if(fileName.startsWith(".") || fileName.startsWith("_"))
				continue;
			
			Reader reader = new Reader(conf, SequenceFile.Reader.file(file));
			
			if(m == null){
				m = (MCLMatrixSlice<?>) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			}
			
			while(reader.next(id, m)){
				long off = nsub * id.get();
				for(SliceEntry e : m.dump()){
					long col = e.col + off;
					writer
					.append(String.valueOf(col))
					.append('\t')
					.append(String.valueOf(e.row))
					.append('\t')
					.append(String.valueOf(e.val));
					writer.newLine();
					nnz++;
				}
			}
			
			num_files++;			
			reader.close();
		}
		
		writer.close();
		outfs.close();
		
		logger.info("{} entries from {} files written",nnz,num_files);
		
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new AbcOutput(), args));
	}

}
