/**
 * 
 */
package io.mat;

import io.writables.TOFPixel;

import java.awt.Dimension;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.jmatio.io.MatFileReader;
import com.jmatio.types.MLArray;
import com.jmatio.types.MLNumericArray;
import com.jmatio.types.MLStructure;

import util.AbstractUtil;

/**
 * @author Cedrik
 *
 */
public class MatFileLoader extends AbstractUtil {

	private static final Logger logger = LoggerFactory.getLogger(MatFileLoader.class);
	
	@Parameter(names = "-te", description = "parallelism, i.e. number of output files in this case")
	private int te = 1;

	@Parameter(names = "-s", description = "start frame (inclusive)")
	private int s = 0;
	
	@Parameter(names = "-t", description = "end frame (exclusive)")
	private int t = -1;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MatFileLoader(), args));
	}

	@Override
	protected int run(Path input, Path output, boolean hdfsOutput)
			throws Exception {
		
		MatFileReader reader = new MatFileReader(input.toString());
		
		Map<String, MLArray> map = reader.getContent();
		
		MLArray arr = map.get("ss0");
		
		if(arr == null){
			logger.warn("no 'ss0' object found in {}. exit",input);
			return 0;
		}
		
		if(!arr.isStruct()){
			logger.warn("'ss0' is not a struct. exit");
			return 0;
		}
		
		MLStructure ss0 = (MLStructure) arr;
		
		if(t == -1){
			t = ss0.getSize();
		}
		
		if(s >= t || s >= ss0.getSize()){
			logger.warn("selection outside frame range. frames: {}, s: {}, t: {}",ss0.getSize(),s,t);
			return 0;
		}
		
		int f = s-t;
		
		logger.info("{} frames selected",f);
		
		int frames_per_thread = f/te;
		int rest = f % te;
		
		int[] off = new int[te+1];		
		for(int i = 1, st = s; i <= te; i++){
			st += i <= rest ? frames_per_thread + 1 : frames_per_thread;
			off[i] = st;
		}
		
		if(off[te] != f){
			logger.error("num frames {} != end of offset {}",f,off[te]);
			return 1;
		}
		
		FileSystem fs = hdfsOutput ? FileSystem.get(getConf()) : FileSystem.getLocal(getConf());
		fs.delete(output, true);
		ExecutorService executorService = Executors.newFixedThreadPool(te);
		List<Future<Long>> futures = new ArrayList<Future<Long>>(te);
		
		for(int i = 0; i < te; i++){
			String fileName = String.format("part-%05d", i);
			Path file = fs.makeQualified(new Path(output,fileName));
			FeatureWriter writer = new FeatureWriter(getConf(), file, ss0, off[i], off[i+1]);
			futures.add(executorService.submit(writer));
		}		
			
		executorService.shutdown();		

		long bytes = 0;
		
		for(Future<Long> future : futures){
			try {
				Long rc = future.get();
				
				if(rc.longValue() == 0L){
					executorService.shutdownNow();
					return 1;
				}
				
				bytes += rc;
			} catch (CancellationException | ExecutionException e) {
				logger.error("exception in writer thread: ",e.getMessage());
				e.printStackTrace();
				executorService.shutdownNow();
				fs.close();
				return 1;
			}
			
		}
		
		FSDataOutputStream meta = fs.create(new Path(output,"_meta"),true);
		meta.writeInt(FeatureWriter.dim.width);
		meta.writeInt(FeatureWriter.dim.height);
		meta.writeInt(f);
		meta.close();
		fs.close();	
		logger.info("{} files with {} bytes total written",te,bytes);
		
		return 0;
	}
	
	private static final class FeatureWriter implements Callable<Long> {
		
		private static Dimension dim = null;
		
		private final Configuration conf;
		private final Path file;
		private final MLStructure ss0;
		private final int s;
		private final int t;
		
		public FeatureWriter(Configuration conf, Path file, MLStructure ss0, int s, int t) {
			this.conf = conf;
			this.file = file;
			this.ss0 = ss0;
			this.s = s;
			this.t = t;
		}

		private static CompressionCodec codec = new Lz4Codec();
		
		private static SequenceFile.Writer createWriter(Configuration conf, Path file) throws IOException{
			return SequenceFile.createWriter(conf,
					SequenceFile.Writer.file(file),
					SequenceFile.Writer.keyClass(IntWritable.class),
					SequenceFile.Writer.valueClass(TOFPixel.class),
					SequenceFile.Writer.compression(CompressionType.BLOCK, codec)
					);
		}
		
		private static synchronized void setDim(MLNumericArray<?> arr){
			if(dim != null)
				return;
			
			dim = new Dimension(arr.getN(), arr.getM());
			logger.info("dim = [width: {}, height: {}}",dim.width,dim.height);
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public Long call() {
			
			try {
				SequenceFile.Writer writer = createWriter(conf, file);
				IntWritable frame = new IntWritable();
				TOFPixel pixel = new TOFPixel();
				
				for(int f = s; f < t; f++){
					frame.set(f);
					final MLNumericArray<Double> X = (MLNumericArray<Double>) ss0.getField("X", f);
					final MLNumericArray<Double> Y = (MLNumericArray<Double>) ss0.getField("Y", f);
					final MLNumericArray<Double> Z = (MLNumericArray<Double>) ss0.getField("Z", f);
					final MLNumericArray<Double> intenSR = (MLNumericArray<Double>) ss0.getField("intenSR", f);
					
					if(dim == null){
						setDim(X);
					}
					
					for(int x = 0, w = dim.width, h = dim.height; x < w; x++){
						for(int y = 0, index = y + h*x; y < h; y++){
							pixel.setP(x, y);
							pixel.setX(X.getReal(index));
							pixel.setY(Y.getReal(index));
							pixel.setZ(Z.getReal(index));
							pixel.setI(intenSR.getReal(index));
							
							writer.append(frame, pixel);
						}
					}					
				}
				
				long bytes = writer.getLength();
				writer.close();
				return bytes;
			} catch (IOException e) {
				logger.error(e.getMessage());
				e.printStackTrace();
				return 0L;
			}
			
		}
		
	}
	
}
