/**
 * 
 */
package mapred.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Cedrik
 *
 */
public class FileUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(FileUtil.class);
	
	public static Class<?>[] checkKeyValueClasses(Configuration conf, FileSystem fs, Path path) throws IOException{
		
		Class<?>[] classes = null;
		
		RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, true);
		
		while(it.hasNext()){
			LocatedFileStatus status = it.next();
			Path file = status.getPath();
			
			if(file.getName().startsWith(".") || file.getName().startsWith("_")){
				continue;
			}
			
			logger.info("check {}",file);
			Reader reader = new Reader(conf, Reader.file(file));
			if(classes == null){
				classes = new Class<?>[]{reader.getKeyClass(),reader.getValueClass()};
			} else {
				if(!classes[0].equals(reader.getKeyClass()) || !classes[1].equals(reader.getValueClass())){
					logger.error("incompatible types: {},{} & {},{}",classes[0],classes[1],reader.getKeyClass(),reader.getValueClass());
					reader.close();
					return null;
				}
			}
			reader.close();
		}
		
		if(classes == null){
			logger.warn("no sequence files found in {}",path);
			return null;
		}
		
		return classes;
	}

}
