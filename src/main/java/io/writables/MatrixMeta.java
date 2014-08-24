package io.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import mapred.Applyable;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatrixMeta implements Writable, Applyable {

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
	
	public long getN(){
		return n;
	}
	
	public int getNSub() {
		return nsub;
	}
	
	public void setKmax(int kmax) {
		this.kmax = kmax < n ? kmax : (int) n;
		logger.debug("kmax set to {}",this.kmax);
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
		Path path = FileOutputFormat.getOutputPath(context);
		FileSystem fs = path.getFileSystem(context.getConfiguration());
		Path output = new Path(path,String.format("%s%05d", KMAX_PREFIX, partition, (short) 1));
		FSDataOutputStream outputStream = fs.create(output);
		outputStream.writeInt(kmax);
		outputStream.close();
		logger.info("kmax {} writen to {}",kmax,output);
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
			logger.debug("from {}: kmax = {}",file,k);
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
		logger.debug("created {} ",meta);
		return meta;
	}
	
	/**
	 * loads MatrixMeta from paths inclusive compatibility check
	 * @param conf
	 * @param path
	 * @return null if path does not contain '.meta' file
	 * @throws IOException
	 */
	public static MatrixMeta load(Configuration conf, Path path) throws IOException {

		FileSystem fs = path.getFileSystem(conf);
		Path src = new Path(path,FILENAME);
		if (!fs.exists(src)) {
			logger.error("{} does not exist",src);
			return null;
		}		
		MatrixMeta meta = new MatrixMeta();				
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
			checkParamaters(m[i]);
			if(first.n != m[i].n || first.nsub != m[i].nsub || first.varints != m[i].varints) {
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
		return String.format("[n: %d, n_sub: %d, k_max: %d, varints: %s]", n,nsub,kmax,varints);
	}

	@Override
	public void apply(Configuration conf) {
		MCLConfigHelper.setN(conf, n);
		MCLConfigHelper.setKMax(conf, kmax);
		MCLConfigHelper.setNSub(conf, nsub);
		MCLConfigHelper.setUseVarints(conf, varints);
		logger.debug("apply {}",this);
	}
}