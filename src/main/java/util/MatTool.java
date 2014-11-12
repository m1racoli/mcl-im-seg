package util;

import java.awt.Dimension;
import java.awt.Point;
import java.awt.Rectangle;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import model.nb.RadialPixelNeighborhood;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.jmatio.io.MatFileReader;
import com.jmatio.types.MLArray;
import com.jmatio.types.MLNumericArray;

public class MatTool extends Configured implements Tool {
	
	private static final Logger logger = LoggerFactory.getLogger(MatTool.class);
	private static final double I_scale = 65535.0;
	private static final double I_scale_squared = I_scale*I_scale;
	
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
	
	@Parameter(names = "-te")
	private int te = 1;
	
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
		
		Frame frame = Frame.fromFile(inFile.toString());
		final int w = frame.w;
		final int h = frame.h;
		logger.info("width: {}, height: {}",w,h);
//		final Raster raster = image.getRaster();
		
		if(analyze){
			logger.info("analyze");
//			WritableRaster raster = image.getRaster();
//			double[] min = new double[3];
//			double[] max = new double[3];
//			double[] val = new double[3];
//			Arrays.fill(min, Double.MAX_VALUE);
//			
//			for(int y = 0; y < raster.getHeight(); y++){
//				for(int x = 0; x < raster.getWidth(); x++){
//					val = raster.getPixel(x, y, val);
//					
//					for(int i = 0; i<3; i++){
//						min[i] = Math.min(min[i], val[i]);
//						max[i] = Math.max(max[i], val[i]);
//					}
//					
//				}
//			}
//			
//			logger.info("max: {} {} {}",max[0],max[1],max[2]);
//			logger.info("min: {} {} {}",min[0],min[1],min[2]);
			
			return 0;
		}
		
		File outFile = new File(output);
		if(outFile.isDirectory()){
			outFile = new File(outFile, "matrix.abc");
		}		
		logger.info("output: {}",outFile);
		
		writeABC(new File(output), frame, new RadialPixelNeighborhood(radius), sigmaX, sigmaF, te);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		MatTool tool = new MatTool();
		JCommander cmd = new JCommander(tool);
		cmd.parse(args);
		System.exit(ToolRunner.run(tool, args));
	}

	public static void writeABC(File file, Frame frame, RadialPixelNeighborhood nb, double a, double b, int te) throws Exception {
		
		ExecutorService executor = Executors.newFixedThreadPool(te);
		List<Future<Iterable<String>>> futures = new ArrayList<Future<Iterable<String>>>(nb.size());
		
		final Dimension dim = new Dimension(frame.w, frame.h);
		System.out.printf("width: %d, height: %d, total: %d, nb: %d\n",dim.width,dim.height, dim.width * dim.height, nb.size());	
		
		if(file.exists())file.delete();
		FileWriter writer = new FileWriter(file);
		
		int edges = 0;
		
		for(Point offset : nb){
			
			OffsetProcessor offsetProcessor = new OffsetProcessor(offset, frame, dim, a, b);
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
		private final Frame frame;
		private final Dimension dim;
		private final double sigmaX;
		private final double sigmaF;
		
		public OffsetProcessor(Point offset, Frame frame, Dimension dim, double sigmaX, double sigmaF) {
			this.offset = new Point(offset);
			this.frame = frame;
			this.dim = dim;
			this.sigmaX = sigmaX;
			this.sigmaF = sigmaF;
		}
		
		@Override
		public Iterable<String> call() throws Exception {
			
			final List<String> results = new ArrayList<String>(dim.height*dim.width);
			final Rectangle area = RadialPixelNeighborhood.getValidRect(offset, dim);
			
			for(int y1 = area.y; y1 < area.y + area.height; y1++){
				for(int x1 = area.x; x1 < area.x + area.width; x1++){
					final int x2 = x1+offset.x;
					final int y2 = y1+offset.y;
					final int i = y1 + dim.height*x1;
					final int j = y2 + dim.height*x2;
					
					final double w = frame.dist(i, j, sigmaX, sigmaF);
					results.add(String.format(Locale.ENGLISH,"%d\t%d\t%f\n", i,j,w));
				}
			}
			return results;
		}
		
	}
	
	private static class Frame {
		
		final MLNumericArray<Double> X;
		final MLNumericArray<Double> Y;
		final MLNumericArray<Double> Z;
		final MLNumericArray<Double> I;
		final int h;
		final int w;
		
		private Frame(MLNumericArray<Double> X,MLNumericArray<Double> Y,MLNumericArray<Double> Z,MLNumericArray<Double> I,int h, int w){
			this.X = X;
			this.Y = Y;
			this.Z = Z;
			this.I = I;
			this.h = h;
			this.w = w;
		}
		
		@SuppressWarnings("unchecked")
		static Frame fromFile(String filename) throws FileNotFoundException, IOException{
			MatFileReader reader = new MatFileReader(filename);
			
			Map<String, MLArray> map = reader.getContent();
			
			MLNumericArray<Double> X = (MLNumericArray<Double>) map.get("X");
			MLNumericArray<Double> Y = (MLNumericArray<Double>) map.get("Y");
			MLNumericArray<Double> Z = (MLNumericArray<Double>) map.get("Z");
			MLNumericArray<Double> I = (MLNumericArray<Double>) map.get("I");
			
			int[] dim = X.getDimensions();
			
			logger.info("frame loaded [h: {}, w: {}]",dim[0],dim[1]);
			
			return new Frame(X,Y,Z,I,dim[0],dim[1]);
		}
		
		double dist(int i1, int i2, double sX, double sF){
			double dX = distSq(X.get(i1),X.get(i2),Y.get(i1),Y.get(i2),Z.get(i1),Z.get(i2))/sX;
			double dI = distSq(I.get(i1),I.get(i2))/(I_scale_squared*sF);
			return Math.exp(-dX-dI);
		}
		
		private static double distSq(double ... v){
			double s = 0.0;
			for(int i = 0; i<v.length; i+=2){
				s += (v[i] - v[i+1])*(v[i] - v[i+1]);
			}
			return s;
		}
		
	}

}
