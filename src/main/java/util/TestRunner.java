/**
 * 
 */
package util;

import io.file.CSVWriter;
import io.file.TextFormatWriter;

import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

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

/**
 * @author Cedrik
 *
 */
public class TestRunner extends Configured implements Tool {

	private static final Logger logger = LoggerFactory.getLogger(TestRunner.class);
	private static final String LOCAL_BASE_PATH = "/mnt/hgfs/mcl-im-seg/results/s%d/";
	private static final Pattern TAB = Pattern.compile("\t");
	
	@Parameter(names = {"-h","--help"}, help = true)
	private boolean help = false;
	
	@Parameter(names = {"-s","--sample"}, required = true)
	private int sample = 0;
	
	@Parameter(names = {"--s3"})
	private boolean s3 = false;
	
	@Parameter(names = {"-a","--algorithm"})
	private List<String> algos = Arrays.asList("mcl");
	
	@Parameter(names = {"-I","--inflation"})
	private List<String> infs = Arrays.asList("2.0");
	
	@Parameter(names = {"-S","--select"})
	private List<String> selects = Arrays.asList("50");
	
	@Parameter(names = {"-sX","--sigmaX"})
	private List<String> sigmaXs = Arrays.asList("1");
	
	@Parameter(names = {"-sF","--sigmaF"})
	private List<String> sigmaFs = Arrays.asList("1");
	
	@Parameter(names = {"-r","--radius"})
	private List<String> radii = Arrays.asList("3");
	
	@Parameter(names = {"-te","--threads"})
	private int te = 1;
	
	@Parameter(names = {"-m","--max-iter"})
	private List<String> max_iters = Arrays.asList("1000");
	
	@Parameter(names = {"-v","-verbose"})
	private boolean verbose = false;
	
	@Parameter(names = {"-b","--base-path"})
	private String base_path = null;
	
	@Parameter(names = {"-o","--output"})
	private String output = null;
	
	private boolean ismat = false;
	
	/* (non-Javadoc)
	 * @see util.AbstractUtil#run(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, boolean)
	 */
	@Override
	public int run(String[] args) throws Exception {
		JCommander cmd = new JCommander(this);
		cmd.addConverterFactory(new PathConverter.Factory());
		cmd.parse(args);
		
		if(help){
			cmd.usage();
			System.exit(1);
		}
		
		File baseDir = base_path == null
				? new File(String.format(LOCAL_BASE_PATH, sample))
				: new File(String.format(base_path+"/s%d/", sample));
		
		if(!baseDir.isDirectory()){
			//TODO			
			return 1;
		}
		
		File srcDir = new File(baseDir, "src");

		if(!srcDir.isDirectory()){
			//TODO
			return 1;
		}
		
		File abcDir = new File(baseDir, "abc");
		File outputDir = new File(baseDir,output == null ? String.valueOf(System.currentTimeMillis()) : output);
		File resultDir = new File(outputDir,"jpg");
		File clusteringDir = new File(outputDir, "clustering");
		abcDir.mkdir();
		clusteringDir.mkdirs();
		resultDir.mkdirs();
		File abcFile = new File(abcDir,"matrix.abc");
		File srcFile = new File(srcDir,srcDir.list()[0]);
		
		ismat = srcFile.getName().endsWith(".mat");
		String inputJob = ismat ? "util.MatTool" : "util.ImageTool";
		
		List<ResultSet> resultSets = new ArrayList<TestRunner.ResultSet>();
				
		for(String r : radii){
			for(String sX : sigmaXs){
				for(String sF : sigmaFs){
					
					int rc = runInput(inputJob, r, sX, sF,srcFile,abcFile,te);
					
					if(rc != 0) return rc;
					
					for(String max_iter : max_iters){
						
						final String max_iter_pattern = String.format("%3d", Integer.valueOf(max_iter));
						for(String algo : algos){
							for(String inf : infs){
								for(String select : selects){
									
									File clusteringFile = new File(clusteringDir, String.format("%s_%s_%s_%s_%s_%s", algo, inf, select, r, sX, sF));
									
									List<String> commands = new ArrayList<String>();
									commands.addAll(Arrays.asList(
											"mcl",
											abcFile.toString(),
											"--abc",
											"-S",select,
											"-R",select,
											"-I",inf,
											"-o",clusteringFile.toString(),
											"-te",String.valueOf(te)));
									
									if(algo.equals("rmcl")) commands.add("--regularized");
									
									ProcessBuilder builder = new ProcessBuilder(commands);
									
									builder.redirectErrorStream(true);
									
									Long end_time = null;
									long start_time = System.currentTimeMillis();
									Process process = builder.start();
									InputStream in = process.getInputStream();
									BufferedReader reader = new BufferedReader(new InputStreamReader(in));
									ParseStatus parseStatus = ParseStatus.HEADER;
									int iterations = 0;
									
									for(String line = reader.readLine(); line != null; line = reader.readLine()){
										switch(parseStatus){
										case HEADER:
											if(line.startsWith(" ite")){
												parseStatus = ParseStatus.MAIN;
											}
											break;
										case MAIN:
											if(line.startsWith("[mcl]")){
												parseStatus = ParseStatus.FOOTER;
												break;
											}
											
											iterations++;
											if(line.startsWith(max_iter_pattern)){
												new ProcessBuilder("bash","-c","kill -s ALRM $(pidof mcl)").inheritIO().start();
											}
											
											break;
										default:
											if(end_time == null) end_time = System.currentTimeMillis();
											break;
										}
										if(verbose)System.out.println(line);
									}
									
									reader.close();
									rc = process.waitFor();
									
									if(rc != 0){
										logger.error("mcl returned with {}",rc);
										return rc;
									}
									
									logger.info("mcl terminated. time: {}, iterations: {}",end_time-start_time,iterations);
															
									ResultSet resultSet = new ResultSet();
									resultSet.algo = algo;
									resultSet.inf = inf;
									resultSet.sX = sX;
									resultSet.sF = sF;
									resultSet.r = r;
									resultSet.S = select;
									resultSet.clusterFile = clusteringFile;
									resultSet.iter = iterations;
									resultSet.time = end_time-start_time;
									resultSet.max_iter = max_iter;
									
									resultSets.add(resultSet);
								}
							}
						}
					}
				}
			}
		}
		
		final BufferedImage image = ismat ? readMatAsImage(srcFile) : ImageIO.read(srcFile);
		
		double[] p = ismat ? new double[1] : new double[3];
		final ColorModel cm = image.getColorModel();
		final double[] zero = new double[cm.getNumColorComponents()];
		for(int i = cm.getNumColorComponents() -1; i>= 0; --i){
			zero[i] = cm.getColorSpace().getMaxValue(i)*(Math.pow(2.0, cm.getComponentSize(i))-1.0);
			//logger.info("component {} -> max = {}",i,zero[i]);
		}
		final WritableRaster original = image.getRaster();
		final int w = image.getWidth();
		final int h = image.getHeight();
		
		File logFile = new File(outputDir, "logfile.csv");
		
		TextFormatWriter out = new CSVWriter(new FileWriter(logFile, true));
		
		for(ResultSet resultSet : resultSets){
			
			int[][] cl_index = new int[w][h];
			
			int clusters = 0;
			BufferedReader reader = new BufferedReader(new FileReader(resultSet.clusterFile));
			
			for(String line = reader.readLine(); line != null; line = reader.readLine()){
				String[] split = TAB.split(line);
				
				for(String str : split){
					Integer idx = Integer.valueOf(str);
					int y = idx % h;
					int x = idx / h;
					cl_index[x][y] = clusters;
				}
				clusters++;
			}
			
			reader.close();
			resultSet.clusters = clusters;
			WritableRaster result = original.createCompatibleWritableRaster();
			
			for(int x = w-1; x >= 0; --x){
				for(int y = h-1; y >= 0; --y){
					if(isBoundary(cl_index, y, x, h, w)){
						result.setPixel(x, y, zero);
					} else {
						result.setPixel(x, y, original.getPixel(x, y, p));
					}
				}
			}
			
			BufferedImage im = new BufferedImage(cm, result, true, null);
			File resultFile = new File(resultDir,resultSet.clusterFile.getName().replaceAll("\\.","")+".jpg");
			ImageIO.write(im, "jpg", resultFile);

			logger.info("{} clusters saved to {}",clusters,resultFile);
			
			resultSet.write(out);
		}
		
		out.close();
		logger.info("logs written to {}",logFile);
		return 0;
	}
	
	private static final boolean isBoundary(int[][] c, int i, int j, int h, int w){
		double v = c[j][i];
		if(i > 0 && v != c[j][i-1]){
			return true;
		} else if ( j > 0 && v != c[j-1][i]){
			return true;
		} else if ( i < h - 1 && v != c[j][i+1]){
			return true;
		} else if ( j < w - 1 && v != c[j+1][i]){
			return true;
		}
		return false;
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new TestRunner(), args));
	}
	
	int runInput(String cls, String r, String sX, String sF, File input, File output, int te) throws IOException, InterruptedException{
		
		ProcessBuilder builder = new ProcessBuilder(
				"mr-mcl",
				cls,
				"-sF",sF,
				"-sX",sX,
				"-r",r,
				"-i",input.toString(),
				"-o",output.toString(),
				"-te",String.valueOf(te),
				"-cielab");
		
		builder.inheritIO();
		Process process = builder.start();
		return process.waitFor();		
	}
	
	private class ResultSet {
		String algo;
		String inf;
		String sX;
		String sF;
		String r;
		String S;
		File clusterFile;
		long time;
		int iter;
		String max_iter;
		int clusters;
		
		void write(TextFormatWriter out) throws IllegalStateException, IOException{
			out.write("algo", algo)
			.write("inf", inf)
			.write("sigmaX", sX)
			.write("sigmaF", sF)
			.write("r", r)
			.write("S", S)
			.write("time", (double) time/1000.0)
			.write("clusters", clusters)
			.write("iter", iter)
			.write("max_iter",max_iter)
			.write("threads",te);
			out.writeLine();
		}
	}
	
	private enum ParseStatus {
		HEADER, MAIN, FOOTER
	}
	
	BufferedImage readMatAsImage(File file) throws IOException{
		
		MatFileReader reader = new MatFileReader(file);
		Map<String, MLArray> map = reader.getContent();
		
		@SuppressWarnings("unchecked")
		MLNumericArray<Double> Z = (MLNumericArray<Double>) map.get("Z");
		int[] dim = Z.getDimensions();
		int h = dim[0];
		int w = dim[1];
		
		double[][] vals = new double[w][h];
		double s0 = 2.3;
		double s1 = 0.0;
		
		for(int j = w-1; j>=0; --j){
			double[] c = vals[j];
			int off = j*h;
			for(int i = h-1; i >= 0; --i){
				double v = Z.getReal(i+off) - s0;
				v /= s1 - s0;
				c[i] = v <= 0.0 ? 0.0 : v >= 1.0 ? 1.0 : v;
			}
		}
		
		BufferedImage image = getGrayScale(vals);
		
		return image;
	}
	
	public static BufferedImage getGrayScale(double[][] vals) {
		
		int w = vals.length;
		int h = vals[0].length;
		byte[] buffer = new byte[w*h];
		
		for(int j = w-1; j >= 0; --j){
			for(int i = h-1; i >= 0; --i){
				buffer[i*w+j] = (byte) (vals[j][i] * 255.0);
			}
		}
		
		ColorSpace cs = ColorSpace.getInstance(ColorSpace.CS_GRAY);
	    int[] nBits = { 8 };
	    ColorModel cm = new ComponentColorModel(cs, nBits, false, true,
	            Transparency.OPAQUE, DataBuffer.TYPE_BYTE);
	    SampleModel sm = cm.createCompatibleSampleModel(w, h);
	    DataBufferByte db = new DataBufferByte(buffer, w * h);
	    WritableRaster raster = Raster.createWritableRaster(sm, db, null);
	    BufferedImage result = new BufferedImage(cm, raster, false, null);
	    
	    return result;
	}

}
