package io.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatrixMeta implements Writable {

	private static final Logger logger = LoggerFactory.getLogger(MatrixMeta.class);
	
	public long n;
	public int n_sub;
	public int k_max;
	
	private MatrixMeta() {}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(n);
		out.writeInt(n_sub);
		out.writeInt(k_max);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		n = in.readLong();
		n_sub = in.readInt();
		k_max = in.readInt();
	}
	
	public <M extends MCLMatrixSlice<M>> void write(Reducer<SliceId, M, SliceId, M>.Context context) throws IOException {
		int partition = context.getConfiguration().getInt(MRJobConfig.TASK_ID, -1);
		Path path = context.getWorkingDirectory();
		FileSystem fs = path.getFileSystem(context.getConfiguration());
		FSDataOutputStream outputStream = fs.create(new Path(path,String.format("_kmax_%5d", partition)), (short) 1);
		write(outputStream);
		outputStream.close();
	}
	
	public static <M extends MCLMatrixSlice<M>> void writeKmax(Reducer<SliceId, M, SliceId, M>.Context context, int kmax) throws IOException {
		int partition = context.getTaskAttemptID().getId();
		Path path = context.getWorkingDirectory();
		FileSystem fs = path.getFileSystem(context.getConfiguration());
		FSDataOutputStream outputStream = fs.create(new Path(path,String.format("%s%5d", KMAX_PREFIX, partition)), (short) 1);
		outputStream.writeInt(kmax);
		outputStream.close();
	}
	
	public void mergeKmax(Configuration conf, Path path) throws IOException {
		int k_max = 0;
		FileSystem fs = path.getFileSystem(conf);
		
		for(FileStatus fileStatus : fs.listStatus(path, pathFilter)) {
			Path file = fileStatus.getPath();
			FSDataInputStream inputStream = fs.open(file);
			int k = inputStream.readInt();
			inputStream.close();
			fs.delete(file, false);
			if(k_max < k) k_max = k;
		}
		
		this.k_max = k_max;
	}
	
	private static final String KMAX_PREFIX = "_kmax_";
	private static final String FILENAME = ".meta";
	
	private static final PathFilter pathFilter = new PathFilter() {
		
		@Override
		public boolean accept(Path path) {
			return path.getName().startsWith(KMAX_PREFIX);
		}
	};
	
	public static MatrixMeta create(long n, int n_sub, int k_max) {
		MatrixMeta meta = new MatrixMeta();
		meta.n = n;
		meta.n_sub = n_sub;
		meta.k_max = k_max;
		return meta;
	}
	
	public static MatrixMeta load(Configuration conf, Path path) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		MatrixMeta meta = new MatrixMeta();
		Path src = new Path(path,FILENAME);
		FSDataInputStream in = fs.open(src);
		meta.readFields(in);			
		in.close();
		logger.debug("loaded {} from {}",meta,src);
		return meta;
	}
	
	public static void save(Configuration conf, Path path, MatrixMeta meta) throws IOException {
		Path dest = new Path(path,FILENAME);
		logger.debug("save {} to {}",meta,dest);
		FileSystem fs = path.getFileSystem(conf);
		FSDataOutputStream out = fs.create(dest);
		meta.write(out);
		out.close();
	}
	
	public static void check(MatrixMeta ... m) {
		
		logger.debug("check {}",Arrays.toString(m));
		
		if(m.length == 0) {
			return;
		}
		
		MatrixMeta first = m[0];
		checkParamaters(first);
		
		for(int i = 1; i < m.length; i++) {
			check(m[i]);
			if(first.n != m[i].n || first.n_sub != m[i].n_sub) {
				throw new RuntimeException("incompatible matrices");
			}
		}

	}
	
	private static void checkParamaters(MatrixMeta m) {
		if(m.n <= 0 || m.n_sub <= 0 || m.k_max < 0){
			throw new RuntimeException(String.format("invalid paramters: {}", m));
		}
	}
	
	@Override
	public String toString() {
		return String.format("[n: %d, n_sub: %d, k_max: %d]", n,n_sub,k_max);
	}
}