package io.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import mapred.MCLConfigHelper;

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
	
	private long n;	
	private int kmax;
	private int nsub;
	private boolean varints;
	
	//TODOMatrixSlice class, threads!
	
	private MatrixMeta() {}
	
	public MatrixMeta(long n, int kmax, int nsub, boolean varints) {
		this.n = n;
		this.kmax = kmax;
		this.nsub = nsub;
		this.varints = varints;
	}
	
	public int getKmax(){
		return kmax;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(n);
		out.writeInt(nsub);
		out.writeInt(kmax);
		out.writeBoolean(varints);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		n = in.readLong();
		nsub = in.readInt();
		kmax = in.readInt();
		varints = in.readBoolean();
	}
	
	public <M extends MCLMatrixSlice<M>> void write(Reducer<SliceId, M, SliceId, M>.Context context) throws IOException {
		int partition = context.getConfiguration().getInt(MRJobConfig.TASK_ID, -1);
		Path path = context.getWorkingDirectory();
		FileSystem fs = path.getFileSystem(context.getConfiguration());
		FSDataOutputStream outputStream = fs.create(new Path(path,String.format("_kmax_%5d", partition)), (short) 1);
		write(outputStream);
		outputStream.close();
	}
	
	public static <M extends MCLMatrixSlice<M>> void writeKmax(Reducer<?, ?, SliceId, M>.Context context, int kmax) throws IOException {
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
		
		this.kmax = k_max;
	}
	
	private static final String KMAX_PREFIX = "_kmax_";
	private static final String FILENAME = ".meta";
	
	private static final PathFilter pathFilter = new PathFilter() {
		
		@Override
		public boolean accept(Path path) {
			return path.getName().startsWith(KMAX_PREFIX);
		}
	};
	
	public static MatrixMeta create(Configuration conf, long n, int kmax) {
		MatrixMeta meta = new MatrixMeta();
		meta.n = n;		
		meta.kmax = kmax;
		meta.nsub = MCLConfigHelper.getNSub(conf);
		meta.varints = MCLConfigHelper.getUseVarints(conf);
		MCLConfigHelper.setN(conf, n);
		MCLConfigHelper.setKMax(conf, kmax);
		return meta;
	}
	
	public static MatrixMeta load(Configuration conf, List<Path> paths) throws IOException {
		return load(conf, (Path[]) paths.toArray());
	}
	
	/**
	 * loads MatrixMeta from paths inclusive compatibility check and write settings to the Configuration
	 * @param conf
	 * @param paths
	 * @return
	 * @throws IOException
	 */
	public static MatrixMeta load(Configuration conf, Path ... paths) throws IOException {
		
		if(paths == null || paths.length == 0) {
			throw new RuntimeException("need at least on path");
		}
		int size = paths.length;
		MatrixMeta[] m = new MatrixMeta[size];
		for(int i = 0; i < size; i++){
			FileSystem fs = paths[i].getFileSystem(conf);
			MatrixMeta meta = new MatrixMeta();
			Path src = new Path(paths[i],FILENAME);
			FSDataInputStream in = fs.open(src);
			meta.readFields(in);			
			in.close();
			logger.debug("loaded {} from {}",meta,src);
			m[i] = meta;
		}
		check(m);		
		MCLConfigHelper.setN(conf, m[0].n);
		MCLConfigHelper.setKMax(conf, m[0].kmax);
		MCLConfigHelper.setNSub(conf, m[0].nsub);
		MCLConfigHelper.setUseVarints(conf, m[0].varints);
		return m[0];
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
			if(first.n != m[i].n || first.nsub != m[i].nsub) {
				throw new RuntimeException("incompatible matrices");
			}
		}

	}
	
	private static void checkParamaters(MatrixMeta m) {
		if(m.n <= 0 || m.nsub <= 0 || m.kmax < 0){
			throw new RuntimeException(String.format("invalid paramters: {}", m));
		}
	}
	
	@Override
	public String toString() {
		return String.format("[n: %d, n_sub: %d, k_max: %d]", n,nsub,kmax);
	}
}