package util;

import io.image.Images;

import java.awt.Dimension;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import mapred.util.FileUtil;
import model.nb.RadialPixelNeighborhood;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
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
	
	@Parameter(names = "-te", description="ignored")
	private int te = 1;
	
	@Parameter(names ="-cielab", description="ignored")
	private boolean cielab= false;
	
	@Parameter(names ="--debug")
	private boolean debug = false;
	
	@Parameter(names = {"-h","--help"}, help = true)
	private boolean help = false;
	
	@Override
	public int run(String[] args) throws Exception {

		if(debug){
			org.apache.log4j.Logger.getRootLogger().setLevel(Level.DEBUG);
		}
		
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
		if(!output.endsWith(".abc")){
			outFile.mkdirs();
			outFile = new File(outFile, "matrix.abc");
		} else {
			outFile.getParentFile().mkdirs();
		}	
		logger.info("output: {}",outFile);
		
		writeABC(outFile, frame, new RadialPixelNeighborhood(radius), sigmaX, sigmaF);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		MatTool tool = new MatTool();
		JCommander cmd = new JCommander(tool);
		cmd.parse(args);
		
		if(tool.help){
			cmd.usage();
			System.exit(1);
		}
		
		System.exit(ToolRunner.run(tool, args));
	}

	public static void writeABC(File file, Frame frame, RadialPixelNeighborhood nb, double a, double b) throws Exception {
		
		final Dimension dim = new Dimension(frame.w, frame.h);
		System.out.printf("width: %d, height: %d, total: %d, nb: %d\n",dim.width,dim.height, dim.width * dim.height, nb.size());	
		
		if(file.exists())file.delete();
		FileWriter writer = new FileWriter(file);
		
		int edges = 0;
		
		for(Point offset : nb){
			
			final Rectangle area = RadialPixelNeighborhood.getValidRect(offset, dim);
			
			for(int x1 = area.x; x1 < area.x + area.width; x1++){
				
				for(int y1 = area.y; y1 < area.y + area.height; y1++){
				
					final int x2 = x1+offset.x;
					final int y2 = y1+offset.y;
					final int i1 = y1 + dim.height*x1;
					final int i2 = y2 + dim.height*x2;
					
					final double w = frame.dist(i1, i2, a, b);
					writer.write(String.format(Locale.ENGLISH,"%d\t%d\t%f\n", i1,i2,w));
					edges++;					
				}
			}
		}
		
		writer.close();
		
		System.out.printf("%d edges written to %s\n",edges,file);
		
	}
	
	private static final String COMPONENT_CONF = "mattool.component";
	private static final String I_MIN_CONF = "mattool.imin";
	private static final String I_MAX_CONF = "mattool.imax";
	
	public static final void setComponent(Configuration conf, String c){
		conf.set(COMPONENT_CONF, c);
	}
	
	public static final void setIMin(Configuration conf, double imin){
		conf.setDouble(I_MIN_CONF, imin);
	}
	
	public static final void setIMax(Configuration conf, double imax){
		conf.setDouble(I_MAX_CONF, imax);
	}
	
	public static BufferedImage readImage(Configuration conf, FileSystem fs, Path file) throws IOException{
		final String c = conf.get(COMPONENT_CONF, "I");
		final double imin = conf.getDouble(I_MIN_CONF, 0.0);
		final double imax = conf.getDouble(I_MAX_CONF, I_scale);
		
		final File localTmp = FileUtil.getLocalCopy(conf, fs, file);
		final Frame frame = Frame.fromFile(localTmp);
		final double[] vals = frame.getComponent(c, imin, imax);
		return Images.createGrayScaleImage(vals, frame.w, frame.h);
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
		
		static Frame fromFile(String filename) throws FileNotFoundException, IOException {
			return fromFile(new File(filename));
		}
		
		@SuppressWarnings("unchecked")
		static Frame fromFile(File file) throws FileNotFoundException, IOException{
			MatFileReader reader = new MatFileReader(file);
			
			Map<String, MLArray> map = reader.getContent();
			
			MLNumericArray<Double> X = (MLNumericArray<Double>) map.get("X");
			MLNumericArray<Double> Y = (MLNumericArray<Double>) map.get("Y");
			MLNumericArray<Double> Z = (MLNumericArray<Double>) map.get("Z");
			MLNumericArray<Double> I = (MLNumericArray<Double>) map.get("I");
			
			int[] dim = X.getDimensions();
			
			logger.info("frame loaded [h: {}, w: {}]",dim[0],dim[1]);
			
			return new Frame(X,Y,Z,I,dim[0],dim[1]);
		}
		
		/**
		 * get component c of frame mapping [imin,imax] -> [0.0,1.0]
		 */
		double[] getComponent(String c, double imin, double imax){

			MLNumericArray<Double> a = null;
			switch(c){
			case "X":
				a = X;
				break;
			case "Y":
				a = Y;
				break;
			case "Z":
				a = Z;
				break;
			case "I":
				a = I;
				break;
			default:
				throw new IllegalArgumentException("unkown component: "+c);				
			}
			
			
			final double c1 = 1.0/(imax-imin);
			final double c0 = imin/(imin-imax);
			final double[] vals = new double[w*h];
			
			for(int i = vals.length -1; i>0; --i){
				final int y = i / w;
				final int x = i % w;
				final double v = a.get(y, x) * c1 + c0;
				vals[i] = v <= 0.0 ? 0.0 : v >= 1.0 ? 1.0 : v;
			}
			
			return vals;
		}
		
		double dist(int i1, int i2, double sX, double sF){
			
			try{
				double dX = distSq(X.getReal(i1),X.getReal(i2),Y.getReal(i1),Y.getReal(i2),Z.getReal(i1),Z.getReal(i2))/sX;
				double dI = distSq(I.getReal(i1),I.getReal(i2))/(I_scale_squared*sF);
				return Math.exp(-dX-dI);
			} catch (Exception e) {
				logger.error("error at i1: {}, i2: {}");
				throw e;
			}
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
