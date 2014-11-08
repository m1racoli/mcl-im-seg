package util;

import java.awt.Dimension;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.imageio.ImageIO;

import model.nb.RadialPixelNeighborhood;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class VideoTool extends Configured implements Tool {
	
	private static final Logger logger = LoggerFactory.getLogger(VideoTool.class);
	
	@Parameter(names = "-i")
	private String input = null;
	
	@Parameter(names = "-o")
	private String output = null;
	
	@Parameter(names = "-r")
	private double radius = 3.0;
	
	@Parameter(names = "-a")
	private double a = 1.0;
	
	@Parameter(names = "-b")
	private double b = 1.0;
	
	@Parameter(names = "-cielab")
	private boolean cielab = false;
	
	@Parameter(names = "-te")
	private int te = 1;
	
	@Parameter(names = "-buffer")
	private int buffer = 8*1024*1034;
	
	@Override
	public int run(String[] args) throws Exception {

		if(input == null || output == null){
			logger.error("specify input and output! {} {}",input, output);
			return 1;
		}
		
		
		File imDir = new File(input);
		if(!imDir.isDirectory()){
			logger.error("{} not a directory",input);
			return 1;
		}
		
		Dimension dim = null;
		List<BufferedImage> images = new ArrayList<BufferedImage>(imDir.list().length);
		
		for(File imFile : imDir.listFiles()){
			logger.info("load {}",imFile);
			BufferedImage image = null;
			try{
				image = ImageIO.read(imFile);
			} catch (IOException e) {}
			
			if(image == null){
				logger.warn("could not load {}", imFile);
				continue;
			}
						
			int w = image.getWidth();
			int h = image.getHeight();
			
			if(dim == null){
				dim = new Dimension(w, h);
			} else {
				if(dim.height != h || dim.width != w){
					logger.warn("dimension missmatch. {}!=[{},{}]. {} ignored",dim,w,h,imFile);
					continue;
				}
			}
			
			if(cielab) image = CIELab.from(image);
			images.add(image);
		}
		
		if(images.isEmpty()){
			logger.error("no image loaded");
			return 1;
		}

		logger.info("images: {}, width: {}, height: {}",images.size(),dim.width,dim.height);
		
		writeABC(new File(output), images, new RadialPixelNeighborhood(radius), dim, a, b, te);	
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		VideoTool tool = new VideoTool();
		JCommander cmd = new JCommander(tool, args);
		System.exit(ToolRunner.run(tool, args));
	}
	
	private static final double r = 3.0;
	private static final double b1 = 0.5;
	private static final double a1 = Math.sqrt(-(r*r)/Math.log(b1));

	public static void writeABC(File file, List<BufferedImage> images, RadialPixelNeighborhood nb, Dimension dim, double a, double b, int te) throws Exception {
		
		int r = (int) nb.getRadius();
		int ims = images.size();
		
		List<Raster> rasters = new ArrayList<Raster>(ims);
		for(BufferedImage im : images)
			rasters.add(im.getRaster());
		ExecutorService executor = Executors.newFixedThreadPool(te);

		System.out.printf("width: %d, height: %d, total: %d, nb: %d\n",dim.width,dim.height, dim.width * dim.height, nb.size());	
		
		if(file.exists())file.delete();
		FileWriter writer = new FileWriter(file);
		
		int edges = 0;
		
		LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(1024*1024*8);
		
		for(Point offset : nb){
			for(int i = 0; i < ims; i++){
				for(int j = Math.max(0, i-r),end = Math.min(ims, i+r+1); j < end ; j++){
					OffsetProcessor offsetProcessor = new OffsetProcessor(offset, rasters, i, j, dim, a, b,queue);
					executor.submit(offsetProcessor);
				}
			}
		}
		
		executor.shutdown();
		String str = null;
		while(!executor.isTerminated()){
			str = queue.poll(1, TimeUnit.SECONDS);
			if(str == null){
				continue;
			}
			writer.write(str);
			edges++;
		}		
		writer.close();
		
		System.out.printf("%d edges written to %s\n",edges,file);
		
	}

	public static class OffsetProcessor implements Runnable {

		private final Queue<String> output;
		private final Point offset;
		private final Raster r1;
		private final Raster r2;
		private final Dimension dim;
		private final double a;
		private final double b;
		private final long c1;
		private final long c2;
		
		public OffsetProcessor(Point offset, List<Raster> rasters, int i1, int i2, Dimension dim, double a, double b, Queue<String> output) {
			this.output = output;
			this.offset = new Point(offset);
			this.r1 = rasters.get(i1);
			this.r2 = rasters.get(i2);
			this.dim = dim;
			this.a = a;
			this.b = b;
			long n = (long) dim.height * (long) dim.width;
			this.c1 = n * (long) i1;
			this.c2 = n * (long) i2;
		}
		
		@Override
		public void run() {
			
			final float[] p1 = new float[4];
			final float[] p2 = new float[4];
			final Rectangle area = RadialPixelNeighborhood.getValidRect(offset, dim);
			final double mds = -offset.distanceSq(0.0, 0.0)/a;
			
			for(int y1 = area.y; y1 < area.y + area.height; y1++){
				for(int x1 = area.x; x1 < area.x + area.width; x1++){
					final int x2 = x1+offset.x;
					final int y2 = y1+offset.y;					
					final long i = (long) x1 + (long) dim.width* (long) y1 + c1;
					final long j = (long) x2 + (long) dim.width* (long) y2 + c2;
					final double d_squared = metric_squared(r1.getPixel(x1, y1, p1), r2.getPixel(x2, y2, p2));
					final double w = Math.exp(mds-(d_squared/b));
					output.offer(String.format(Locale.ENGLISH,"%d\t%d\t%f\n", i,j,w));
				}
			}
		}
		
	}

	public static void writeABC(File file, BufferedImage image, RadialPixelNeighborhood nb, final double scale) throws IOException {
		
		final Raster raster = image.getData();
		final Dimension dim = new Dimension(image.getWidth(), image.getHeight());
		System.out.printf("width: %d, height: %d, total: %d, nb: %d\n",dim.width,dim.height, dim.width * dim.height, nb.size());	
		
		
		FileWriter writer = new FileWriter(file);
		float[] p1 = new float[4];
		float[] p2 = new float[4];
		
		int edges = 0;
		
		for(Point offset : nb){
			
			final Rectangle area = RadialPixelNeighborhood.getValidRect(offset, dim);
			final double mds = -offset.distanceSq(0.0, 0.0)/a1;
			
			for(int y1 = area.y; y1 < area.y + area.height; y1++){
				for(int x1 = area.x; x1 < area.x + area.width; x1++){
					final int x2 = x1+offset.x;
					final int y2 = y1+offset.y;					
					final long i = x1 + dim.width*y1;
					final long j = x2 + dim.width*y2;
					final double d_squared = metric_squared(raster.getPixel(x1, y1, p1), raster.getPixel(x2, y2, p2));
					final double w = Math.exp(mds-d_squared/scale);
					writer.write(String.format(Locale.ENGLISH,"%d\t%d\t%f\n", i,j,w));
					edges++;
				}
			}
		}
		
		writer.close();
		
		System.out.printf("%d edges written to %s\n",edges,file);
		
	}
	
	private static final double metric_squared(float[] v1, float[] v2){
		double sum = 0.0;
		for(int i = 0; i < v1.length; i++){
			double v = (v1[i]-v2[i]);
			sum += v*v;
		}
		return sum;
	}
	
	private static final float[] D50 = {0.3457f,0.3585f,0.2958f};
	private static final float[] D50_n = {D50[0]/D50[1],D50[1]/D50[1],D50[2]/D50[1]};
	
	private static final float deltaE_CIE76_XYZ_sqared(float[] xyz1, float[] xyz2){
		final float[] lab1 = toCIELAB(xyz1);
		final float[] lab2 = toCIELAB(xyz2);
		return	(lab1[0] - lab2[0]) * (lab1[0] - lab2[0]) +
				(lab1[1] - lab2[1]) * (lab1[1] - lab2[1]) +
				(lab1[2] - lab2[2]) * (lab1[2] - lab2[2]);
	}
	
	private static final float[] toCIELAB(float[] xyz){
		float[] f = {f(xyz[0]/D50_n[0]),f(xyz[1]/D50_n[1]),f(xyz[2]/D50_n[2])};
		return new float[] {
				116.0f * f[1]-16.0f,
				500 * (f[0] - f[1]),
				200 * (f[1] - f[2])};
	}
	
	private static final float f(float t){
		return (float) (t > Math.pow(6/29,3) ? Math.pow(t,1.0/3.0) : t * Math.pow(29.0/6.0,2)/3.0 + 4.0/29.0) ;
	}	
	
	public static BufferedImage readClusters(File file, BufferedImage src) throws IOException{
		
		final Raster srcRaster = src.getData();
		final WritableRaster destRaster = srcRaster.createCompatibleWritableRaster();
		
		final int w = src.getWidth();
		final int h = src.getHeight();
		
		Random rnd = new Random(0);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		
		int cluster = 0;
		for(String line = reader.readLine(); line != null; line = reader.readLine()){
			
			final String[] split = line.split("\\t");
			final int[] xs = new int[split.length];
			final int[] ys = new int[split.length];
			
			int[] avg = null;
			int[] add = null;
			
			for(int i = 0; i < split.length; i++){
				final int index = Integer.parseInt(split[i]);
				final int x = w > h ? index / h : index % w;
				final int y = w > h ? index % h : index / w;
				xs[i] = x;
				ys[i] = y;
				
				if(avg == null){
					avg = srcRaster.getPixel(x, y, avg);
				} else {
					add = srcRaster.getPixel(x, y, add);
					add1(avg, add);
				}
			}
			
			div(avg, split.length);
			
			int v = rnd.nextInt(256);
			
			Arrays.fill(avg, v);
			
			for(int i = 0; i< xs.length;i++){
				destRaster.setPixel(xs[i], ys[i], avg);
			}
			
		}
		
		reader.close();
		
		BufferedImage resultImage = new BufferedImage(src.getColorModel(), destRaster, true, null);
		
		return resultImage;
	}
	
	public static final void add1(int[] v1, int[] v2){
		for(int j = 0; j < v1.length; j++)
			v1[j] += v2[j];
	}
	
	public static final void div(int[] v, int n){
		for(int j = 0; j < v.length; j++)
			v[j] /= n;
	}
	
	public static final void avg(int[] v){
		int a = 0;
		for(int i = 0; i<v.length;i++)
			a += v[i];
		
		a /= v.length;
		for(int i = 0; i<v.length;i++)
			v[i] = a;		
	}

}
