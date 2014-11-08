/**
 * 
 */
package io.image;

import io.writables.Pixel;

import java.awt.Dimension;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

import util.AbstractUtil;
import util.CIELab;

/**
 * @author Cedrik
 *
 */
public class ImageFileLoader extends AbstractUtil {

	private static final Logger logger = LoggerFactory.getLogger(ImageFileLoader.class);
	
	@Parameter(names = "-te", description = "parallelism, i.e. number of threads and output files in this case")
	private int te = 1;

	@Parameter(names = "-s", description = "start frame (inclusive)")
	private int s = 0;
	
	@Parameter(names = "-t", description = "end frame (exclusive)")
	private int t = Integer.MAX_VALUE;
	
	@Parameter(names = "-cielab")
	private boolean cielab = false;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ImageFileLoader(), args));
	}

	@Override
	protected int run(Path input, Path output, boolean hdfsOutput)
			throws Exception {

		//TODO support multiple images
		
		FileSystem inFS = input.getFileSystem(getConf());
		BufferedImage image = ImageIO.read(inFS.open(input));
		
		if(cielab){
			logger.info("transform to cielab space");
			image = CIELab.from(image);
		}
		
		final Dimension dim = new Dimension(image.getWidth(), image.getHeight());
		
		logger.info("{} images with dim=[w={},h={}]",1,dim.getWidth(),dim.getHeight());
		
		FileSystem fs = hdfsOutput ? output.getFileSystem(getConf()) : FileSystem.getLocal(getConf());		
		fs.delete(output, true);
		
		int rows_per_thread = dim.height/te;
		int rest = dim.height % te;
		
		int[] off = new int[te+1];
		off[0] = 0;
		
		for(int i = 1, st = 0; i <= te; i++){
			st += i <= rest ? rows_per_thread + 1 : rows_per_thread;
			off[i] = st;
		}
		
		if(off[te] != dim.getHeight()){
			logger.error("num rows {} != end of offset {}",dim.getHeight(),off[te]);
			fs.close();
			return 1;
		}
		
		ExecutorService executorService = Executors.newFixedThreadPool(te);
		List<Future<Long>> futures = new ArrayList<Future<Long>>(te);		
		
		for(int i = 0; i < te; i++){
			String fileName = String.format("part-%05d", i);
			Path file = fs.makeQualified(new Path(output,fileName));
			FeatureWriter writer = new FeatureWriter(getConf(), file, image, off[i], off[i+1], dim);
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
		meta.writeInt(dim.width);
		meta.writeInt(dim.height);
		meta.writeInt(1);
		meta.close();
		fs.close();	
		logger.info("{} files with {} bytes total written",te,bytes);
		
		return 0;
	}
	
	private static final class FeatureWriter implements Callable<Long> {
		
		private final Dimension dim;		
		private final Configuration conf;
		private final Path file;
		private final BufferedImage image;
		private final int s;
		private final int t;
		
		public FeatureWriter(Configuration conf, Path file, BufferedImage image, int s, int t, Dimension dim) {
			this.conf = conf;
			this.file = file;
			this.image = image;
			this.s = s;
			this.t = t;
			this.dim = dim;
		}

		private static CompressionCodec codec = new DefaultCodec();
		
		private static SequenceFile.Writer createWriter(Configuration conf, Path file) throws IOException{
			return SequenceFile.createWriter(conf,
					SequenceFile.Writer.file(file),
					SequenceFile.Writer.keyClass(IntWritable.class),
					SequenceFile.Writer.valueClass(Pixel.class),
					SequenceFile.Writer.compression(CompressionType.BLOCK, codec)
					);
		}
		
		@Override
		public Long call() {
			
			try {
				WritableRaster raster = image.getRaster();
				SequenceFile.Writer writer = createWriter(conf, file);
				IntWritable frame = new IntWritable(1);
				Pixel pixel = new Pixel();
				
				for(int y = s; y < t; y++){
					
					pixel.setY(y);
					
					for(int x = dim.width - 1; x >= 0; x--){						
						pixel.setX(x);
						raster.getPixel(x, y, pixel.getF());						
						writer.append(frame, pixel);
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
