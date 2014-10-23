/**
 * 
 */
package io.matrix;

import java.util.Iterator;
import io.writables.SliceId;
import iterators.ReadOnlyIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Cedrik
 *
 */
public class DistributedMCLMatrix extends MCLMatrix {

	private final Path path;
	private final Configuration conf;
	
	public DistributedMCLMatrix(Configuration conf, FileSystem fs, Path path) {
		this.path = fs.makeQualified(path);
		this.conf = conf;
	}

	@Override
	public Iterator<MatrixEntry> iterator() {
		return new ReadOnlyIterator<MatrixEntry>(){
			
			private final SliceId id = new SliceId();

			@Override
			public boolean hasNext() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public MatrixEntry next() {
				// TODO Auto-generated method stub
				return null;
			}
			
		};
	}
	
	

}
