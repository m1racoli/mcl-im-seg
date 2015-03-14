/**
 * 
 */
package util;

import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import javax.imageio.ImageIO;

import io.cluster.ArrayClustering;
import io.cluster.Cluster;
import io.cluster.Clustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class VisualizeClusters extends AbstractUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(VisualizeClusters.class);
	
	@Parameter(names = "-c", required = true, converter = PathConverter.class, description = "clustering")
	private Path clusteringFile = null;
	
	@Parameter(names = {"-f","--format"}, description = "format to save result in")
	private String output_format = "jpg";
	
	@Parameter(names = {"-lc","--line-color"})
	private double line_color = 1.0;
	
	@Parameter(names = {"--component"}, description = "component of matfile to choose (X,Y,Z,I)")
	private String component = "I";
	
	@Parameter(names = {"--imin"}, description = "I min threshold of mat file component")
	private double imin = 0.0;
	
	@Parameter(names = {"--imax"}, description = "I max threshold of mat file component")
	private double imax = 65535.0;
	
	/* (non-Javadoc)
	 * @see util.AbstractUtil#run(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, boolean)
	 */
	@Override
	protected int run(Path input, Path output, boolean hdfsOutput)
			throws Exception {

		MatTool.setComponent(getConf(), component);
		MatTool.setIMin(getConf(), imin);
		MatTool.setIMax(getConf(), imax);
		
		FileSystem fs = clusteringFile.getFileSystem(getConf());
		clusteringFile = fs.makeQualified(clusteringFile);
		Clustering<Integer> clustering = new ArrayClustering(new InputStreamReader(fs.open(clusteringFile)));
		logger.info("clustering with {} clusters loaded",clustering.size());
		
		// load input images
		final BufferedImage[] images = loadImages(getConf(), input.getFileSystem(getConf()), input);		
		if(images == null || images.length == 0){return 1;}
		
		// prepare output
		FileSystem outFS = output.getFileSystem(getConf());		
		if(outFS.exists(output)){outFS.delete(output, true);}
		outFS.mkdirs(output);
		
		// prepare output images
		final int l = images.length;
		final Raster[] rasters = new Raster[l];
		final ColorModel[] cms = new ColorModel[l];
		final WritableRaster[] outRasters = new WritableRaster[l];
		for(int i = 0; i < l; i++){
			BufferedImage im = images[i];
			rasters[i] = im.getRaster();
			cms[i] = im.getColorModel();
			outRasters[i] = im.getRaster().createCompatibleWritableRaster();
		}
		
		final int w = images[0].getWidth();
		final int h = images[0].getHeight();
		final int n = w*h;
		final int nc = cms[0].getNumColorComponents();
		final NBTest nbtest = new NBTest(w, h);
		
		logger.info("{} images loaded",l);
		
		final double[] tmp = new double[nc];
		final double[] line_pixel = new double[nc];
		Arrays.fill(line_pixel, 0.0);
		
		for(Cluster<Integer> cl : clustering){
			
			for(Integer i : cl){
				
				final int f = i / n;
				final int sub_i = i % n;
				final int y = sub_i % h;
				final int x = sub_i / h;
				
				if(nbtest.isInner(sub_i, x, y, cl, clustering)){
					outRasters[f].setPixel(x, y, rasters[f].getPixel(x, y, tmp));
				} else {
					outRasters[f].setPixel(x, y, line_pixel);
				}
			}
		}
		
		logger.info("{} clusters processed",clustering.size());
		
		for(int f = 0; f < l; f++){
			final BufferedImage im = new BufferedImage(cms[f], outRasters[f], true, null);
			final Path outfile = outFS.makeQualified(new Path(output,String.format("part-%05d.%s",f,output_format)));
			FSDataOutputStream out = outFS.create(outfile, true);
			ImageIO.write(im, output_format, out);
			logger.info("output {} written",outfile);
			out.close();
		}
		
		logger.info("visualizations saved");
		
		return 0;
	}

	private static class NBTest {
		
		private final int w;
		private final int h;
		
		NBTest(int w, int h){
			this.w = w;
			this.h = h;
		}
		
		boolean isInner(int i, int x , int y, Cluster<Integer> cl, Clustering<Integer> clustering){
			
			if(y > 0 && !clustering.getCluster(i-1).equals(cl))
				return false;
			if(y < h - 1 && !clustering.getCluster(i+1).equals(cl))
				return false;
			if(x > 0 && !clustering.getCluster(i-h).equals(cl))
				return false;
			if(x < w - 1 && !clustering.getCluster(i+h).equals(cl))
				return false;
			return true;
		}
	}
	
	private static BufferedImage[] loadImages(Configuration conf, FileSystem fs, Path input) throws IOException {
		
		if(!fs.isDirectory(input)){
			return new BufferedImage[]{loadImage(conf, fs, input)};
		}
		
		final FileStatus[] status = fs.listStatus(input, new ImagePathFiler());
		final BufferedImage[] ims = new BufferedImage[status.length];
		
		for(int i = 0; i < status.length; i++){
			ims[i] = loadImage(conf, fs, status[i].getPath());
		}
		
		return ims;
	}
	
	private static BufferedImage loadImage(Configuration conf, FileSystem fs, Path input) throws IOException {
		if(input.getName().toLowerCase().endsWith(".mat")){
			return MatTool.readImage(conf, fs, input);
		}
		logger.info("read image {}",input);
		return ImageIO.read(fs.open(input));
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new VisualizeClusters(), args));
	}
	
	public static class ImageFileFilter implements FilenameFilter {

		@Override
		public boolean accept(File dir, String name) {
			return name.endsWith(".jpg");
		}
		
	}
	
	public static class ImagePathFiler implements PathFilter {

		@Override
		public boolean accept(Path path) {
			final String name = path.getName().toLowerCase();
			return name.endsWith(".jpg") || name.endsWith(".png") || name.endsWith(".mat");
		}
		
	}

}
