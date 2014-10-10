/**
 * 
 */
package util;

import io.cluster.DefaultImageClustering;
import io.cluster.ImageCluster;
import io.cluster.ImageClustering;
import io.image.Images;

import java.awt.Point;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class Summarize extends AbstractUtil {
	
	private static final Pattern PATTERN = Pattern.compile("_");
	private static final Pattern LINE_PATTERN = Pattern.compile("[ ]+");
	
	@Parameter(names = "-p", required = true)
	private String pic = null;
	
	@Parameter(names = "-l")
	private String logs = null;
	
	@Parameter(names = "-cielab")
	private boolean cielab = false;
	
	@Parameter(names = "-te")
	private int te = 1;
	
	@Parameter(names = "-warn-dist")
	private double warn_dist = 10000;
	
	/* (non-Javadoc)
	 * @see util.AbstractUtil#run(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, boolean)
	 */
	@Override
	protected int run(Path input, Path output, boolean hdfsOutput)
			throws Exception {		
		
		File imFile = new File(pic);
		File dir = new File(input.toString());
		if(!dir.exists() || !dir.isDirectory() || !imFile.exists()){
			System.err.printf("invalid inputs: dir=%s, image=%s",dir, imFile);
			return 1;
		}
		
		System.out.printf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s","a","b","r","inf","num_clusters","avg_cl_size","max_cl_size","max_inhomo","avg_inhomo","max_cl_inhomo","avg_cl_inhomo");
		if(logs != null) System.out.printf(",%s,%s", "time","iter");
		System.out.println();
		
		BufferedImage im = ImageIO.read(imFile);
		if(cielab) im = CIELab.from(im);
		Raster raster = im.getRaster();
		
		final int w = im.getWidth();
		final int h = im.getHeight();
		final int n = w*h;
		
		for(File file : dir.listFiles()){
			
			String[] split = PATTERN.split(file.getName(), 4);
			
			int a = Integer.parseInt(split[0]);
			int b = Integer.parseInt(split[1]);
			int r = Integer.parseInt(split[2]);			
			double inf = Double.parseDouble(split[3].substring(0, 3));
			
			ImageClustering clustering = new DefaultImageClustering(file, w, h);
			int size = clustering.size();
			
			double avg_cl_size = 0.0;
			int max_cl_size = 0;
			double max_inhomoheneity = 0.0;
			double avg_inhomogeneity = 0.0;
			double max_cl_inhomogeneity = 0.0;
			double avg_cl_inhomogeneity = 0.0;
			double[] v = null;
			//ImageClusterings.testInverse(clustering);
			//double avg_ncut = te == 1 ? ImageClusterings.nCut(clustering, raster) : ImageClusterings.nCut(clustering, raster,te);
			//avg_ncut /= size;
			
			for(ImageCluster cluster : clustering){
				int cl_size = 0;
				double[] c = Images.getAvgPixel(raster, cluster);
				double cl_inhomogeneity = 0.0;
				for(Point p : cluster) {
					v = raster.getPixel(p.x, p.y, v);
					double dist = Images.dist(c,v);
					if(dist > warn_dist){
						System.err.printf("warn dist=%f: center=(%f,%f,%f) and pixel=(%f,%f,%f)\n", dist,c[0],c[1],c[2],v[0],v[1],v[2]);
					}
					max_inhomoheneity = Math.max(max_inhomoheneity, dist);
					avg_inhomogeneity += dist;
					cl_inhomogeneity += dist;
					cl_size++;
				}
				avg_cl_size += cl_size;
				max_cl_size = Math.max(max_cl_size, cl_size);
				cl_inhomogeneity /= cl_size;
				max_cl_inhomogeneity = Math.max(max_cl_inhomogeneity, cl_inhomogeneity);
				avg_cl_inhomogeneity += cl_inhomogeneity;
			}
			
			avg_cl_size /= size;
			avg_inhomogeneity /= n;
			avg_cl_inhomogeneity /= size;
			
			System.out.printf("%d,%d,%d,%f,%d,%f,%d,%f,%f,%f,%f",a,b,r,inf,size,avg_cl_size,max_cl_size,max_inhomoheneity,avg_inhomogeneity,max_cl_inhomogeneity,avg_cl_inhomogeneity);
			if(logs != null){
				
				printLog(new File(logs, file.getName()));
			}
			System.out.println();
			
		}
		
		return 0;
	}
	
	private static final void printLog(File file) {
		
		if(!file.exists() || file.isDirectory()){
			System.out.printf(",%s,%s", null, null);
			return;
		}
		
		try {
		
			BufferedReader reader = new BufferedReader(new FileReader(file));
			
			ParseStatus parseStatus = ParseStatus.HEADER;
			
			int iter = 0;
			double time = 0.0;
			
			OUTER: for(String line = reader.readLine(); line != null; line = reader.readLine()){
				switch(parseStatus){			
				case HEADER:
					if(line.startsWith(" ite")){
						parseStatus = ParseStatus.MAIN;
					}
					break;
				case MAIN:
					if(line.startsWith("[mcl]")){
						parseStatus = ParseStatus.FOOTER;
						break OUTER;
					}
					
					iter++;
					String[] split = LINE_PATTERN.split(line, 5);
					time += Double.parseDouble(split[iter < 100 ? 3 : 2]);
					break;
				default:
					break OUTER;
				}
			}
			reader.close();
			System.out.printf(",%f,%d", time, iter);
		
		} catch (IOException e) {
			System.out.printf(",%s,%s", null, null);
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Summarize(), args));
	}
	
	private enum ParseStatus {
		HEADER, MAIN, FOOTER
	}

}
