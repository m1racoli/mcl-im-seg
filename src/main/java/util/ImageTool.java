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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.imageio.ImageIO;

import model.nb.RadialPixelNeighborhood;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ImageTool extends Configured implements Tool {
	
	private static final Logger logger = LoggerFactory.getLogger(ImageTool.class);
	
	@Parameter(names = "-i")
	private String input = null;
	
	@Parameter(names = "-o")
	private String output = null;
	
	@Parameter(names = "-a")
	private boolean analyze = false;
	
	@Parameter(names = "-r")
	private double radius = 3.0;
	
	@Parameter(names = "-sX")
	private double sigmaX = 1.0;
	
	@Parameter(names = "-sF")
	private double sigmaF = 1.0;
	
	@Parameter(names = "-cielab")
	private boolean cielab = false;
	
	@Parameter(names = "-te")
	private int te = 1;
	
	@Parameter(names = {"-h","--help"}, help = true)
	private boolean help = false;
	
	@Override
	public int run(String[] args) throws Exception {

		if(input == null){
			logger.error("specify input! {}",input);
			return 1;
		}
		
		if(!analyze && output == null){
			logger.error("specify output! {}",output);
			return 1;
		}
		
		logger.info("load image {}",input);
		File inFile = new File(input);
		if(inFile.isDirectory()){
			String[] list = inFile.list();
			if(list == null || list.length == 0){
				logger.error("no files found in {}",input);
				return 1;
			}
			
			if(list.length > 1){
				logger.warn("take only the first of {} files",list.length);
			}
			
			inFile = new File(inFile,list[0]);
		}
		
		logger.info("input: {}",inFile);
		
		BufferedImage image = ImageIO.read(inFile);
		if(cielab) image = CIELab.from(image);
		final int w = image.getWidth();
		final int h = image.getHeight();
		logger.info("width: {}, height: {}",w,h);
//		final Raster raster = image.getRaster();
		
		if(analyze){
			logger.info("analyze");
			WritableRaster raster = image.getRaster();
			double[] min = new double[3];
			double[] max = new double[3];
			double[] val = new double[3];
			Arrays.fill(min, Double.MAX_VALUE);
			
			for(int y = 0; y < raster.getHeight(); y++){
				for(int x = 0; x < raster.getWidth(); x++){
					val = raster.getPixel(x, y, val);
					
					for(int i = 0; i<3; i++){
						min[i] = Math.min(min[i], val[i]);
						max[i] = Math.max(max[i], val[i]);
					}
					
				}
			}
			
			logger.info("max: {} {} {}",max[0],max[1],max[2]);
			logger.info("min: {} {} {}",min[0],min[1],min[2]);
			
			return 0;
		}
		
		File outFile = new File(output);
		
		if(!output.endsWith(".abc")){
			outFile.mkdirs();
			outFile = new File(outFile, "matrix.abc");
		} else {
			outFile.getParentFile().mkdirs();
		}
	
		logger.info("output: {}",outFile);
		
		if(te > 1) writeABC(outFile, image, new RadialPixelNeighborhood(radius), sigmaX, sigmaF, te);
		else writeABC(outFile, image, new RadialPixelNeighborhood(radius), sigmaX, sigmaF);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ImageTool tool = new ImageTool();
		JCommander cmd = new JCommander(tool, args);
		
		if(tool.help){
			cmd.usage();
			System.exit(1);
		}
		
		System.exit(ToolRunner.run(tool, args));
	}
	
	private static final double r = 3.0;
	private static final double b1 = 0.5;
	private static final double a1 = Math.sqrt(-(r*r)/Math.log(b1));
	
	public static void writeABC(File file, BufferedImage image, RadialPixelNeighborhood nb, double sigmaX, double sigmaF) throws IOException {
		
		final Raster raster = image.getData();
		final Dimension dim = new Dimension(image.getWidth(), image.getHeight());
		System.out.printf("width: %d, height: %d, total: %d, nb: %d\n",dim.width,dim.height, dim.width * dim.height, nb.size());	
		
		if(file.exists())file.delete();
		FileWriter writer = new FileWriter(file);
		double[] p1 = new double[3];
		double[] p2 = new double[3];
		
		int edges = 0;
		
		for(Point offset : nb){
			
			final Rectangle area = RadialPixelNeighborhood.getValidRect(offset, dim);
			final double mds = -offset.distanceSq(0.0, 0.0)/sigmaX;
			
			for(int y1 = area.y; y1 < area.y + area.height; y1++){
				for(int x1 = area.x; x1 < area.x + area.width; x1++){
					final int x2 = x1 + offset.x;
					final int y2 = y1 + offset.y;					
					final long i = y1 + dim.height*x1;
					final long j = y2 + dim.height*x2;
					final double d_squared = metric_squared(raster.getPixel(x1, y1, p1), raster.getPixel(x2, y2, p2));
					final double w = Math.exp(mds-(d_squared/sigmaF));
					writer.write(String.format(Locale.ENGLISH,"%d\t%d\t%f\n", i,j,w));
					edges++;
				}
			}
		}
		
		writer.close();
		
		System.out.printf("%d edges written to %s\n",edges,file);
		
	}

	public static void writeABC(File file, BufferedImage image, RadialPixelNeighborhood nb, double a, double b, int te) throws Exception {
		
		ExecutorService executor = Executors.newFixedThreadPool(te);
		List<Future<Iterable<String>>> futures = new ArrayList<Future<Iterable<String>>>(nb.size());
		
		final Raster raster = image.getData();
		final Dimension dim = new Dimension(image.getWidth(), image.getHeight());
		System.out.printf("width: %d, height: %d, total: %d, nb: %d\n",dim.width,dim.height, dim.width * dim.height, nb.size());	
		
		if(file.exists())file.delete();
		FileWriter writer = new FileWriter(file);
		
		int edges = 0;
		
		for(Point offset : nb){
			
			OffsetProcessor offsetProcessor = new OffsetProcessor(offset, raster, dim, a, b);
			futures.add(executor.submit(offsetProcessor));
		}
		
		executor.shutdown();
		
		Iterator<Future<Iterable<String>>> iter = futures.iterator();
		
		
		
		while(iter.hasNext()){
			for(String str : iter.next().get()){
				writer.write(str);
				edges++;
			}
			iter.remove();
		}
		
		writer.close();
		
		System.out.printf("%d edges written to %s\n",edges,file);
		
	}

	public static class OffsetProcessor implements Callable<Iterable<String>> {

		private final Point offset;
		private final Raster raster;
		private final Dimension dim;
		private final double a;
		private final double b;
		
		public OffsetProcessor(Point offset, Raster raster, Dimension dim, double a, double b) {
			this.offset = new Point(offset);
			this.raster = raster;
			this.dim = dim;
			this.a = a;
			this.b = b;
		}
		
		@Override
		public Iterable<String> call() throws Exception {
			
			final List<String> results = new ArrayList<String>(dim.height*dim.width);
			final double[] p1 = new double[3];
			final double[] p2 = new double[3];
			final Rectangle area = RadialPixelNeighborhood.getValidRect(offset, dim);
			final double mds = -offset.distanceSq(0.0, 0.0)/a;
			
			for(int y1 = area.y; y1 < area.y + area.height; y1++){
				for(int x1 = area.x; x1 < area.x + area.width; x1++){
					final int x2 = x1+offset.x;
					final int y2 = y1+offset.y;					
					final long i = y1 + dim.height*x1;
					final long j = y2 + dim.height*x2;
					final double d_squared = metric_squared(raster.getPixel(x1, y1, p1), raster.getPixel(x2, y2, p2));
					final double w = Math.exp(mds-(d_squared/b));
					results.add(String.format(Locale.ENGLISH,"%d\t%d\t%f\n", i,j,w));
				}
			}
			return results;
		}
		
	}

	public static void writeABC(File file, BufferedImage image, RadialPixelNeighborhood nb, final double scale) throws IOException {
		
		final Raster raster = image.getData();
		final Dimension dim = new Dimension(image.getWidth(), image.getHeight());
		System.out.printf("width: %d, height: %d, total: %d, nb: %d\n",dim.width,dim.height, dim.width * dim.height, nb.size());	
		
		
		FileWriter writer = new FileWriter(file);
		double[] p1 = new double[3];
		double[] p2 = new double[3];
		
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
	
	private static final double metric_squared(double[] v1, double[] v2){
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
