/**
 * 
 */
package util;

import java.awt.Dimension;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Random;

import javax.imageio.ImageIO;

import io.cluster.ArrayClustering;
import io.cluster.Cluster;
import io.cluster.Clustering;
import io.image.Images;

import org.apache.hadoop.fs.Path;
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
	
	@Parameter(names = "-te")
	private int te = 1;
	
	@Parameter(names = "-c", required = true)
	private String clustering = null;
		
	/* (non-Javadoc)
	 * @see util.AbstractUtil#run(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, boolean)
	 */
	@Override
	protected int run(Path input, Path output, boolean hdfsOutput)
			throws Exception {

		File clusteringFile = new File(clustering);
		Clustering<Integer> clustering = new ArrayClustering(clusteringFile);
		logger.info("clustering loaded");
		
		File inputFile = new File(input.toString());
		
		if(!inputFile.isDirectory()){
			logger.error("{} not a directory",inputFile);
			return 1;
		}
		
		File outDir = new File(output.toString());
		outDir.mkdirs();
		
		if(!outDir.isDirectory()){
			logger.error("could not create output dir: {}",outDir);
			return 1;
		}
		
		final File[] imFiles = inputFile.listFiles(new ImageFileFilter()); //TODO image file types
		final int l = imFiles.length;
		final Raster[] rasters = new Raster[l];
		final ColorModel[] cms = new ColorModel[l];
		
		Dimension dim = null;
		
		for(int i = 0; i < l; i++){
			File imFile = imFiles[i];
			BufferedImage im = ImageIO.read(imFile);
			rasters[i] = im.getRaster();
			cms[i] = im.getColorModel();
			if(dim == null){
				dim = new Dimension(im.getWidth(), im.getHeight());
			} else {
				if(dim.width != im.getWidth() || dim.height != im.getHeight()){
					throw new RuntimeException("dimensions dont match previous images: "+imFile);
				}
			}
		}
		
		final int w = dim.width;
		final int h = dim.height;
		final int n = w*h;
		dim=null;
		
		WritableRaster[] outRasters = new WritableRaster[l];
		for(int i = 0; i < l; i++){
			outRasters[i] = rasters[i].createCompatibleWritableRaster();
		}
		
		logger.info("{} images loaded",l);
		
		double[] avg = new double[3];
		double[] add = new double[3];
		Random rand = new Random(3141L);
		for(Cluster<Integer> cl : clustering){
			
			Arrays.fill(avg, 0.0);
			
			for(int i : cl){
				int z = i/n;
				i = i % n;
				final int x = i%w;//w > h ? i / h : i % w;
				final int y = i/w;//w > h ? i % h : i / w;
				
				add = rasters[z].getPixel(x, y, add);
				Images.addToFirst(avg, add);
			}
			
			Images.div(avg, cl.size());
			Images.add(avg, rand.nextInt(19)-9); //add jitter to the cluster colors	
			Images.bound(avg); //keep values in range
			
			for(int i : cl){
				int z = i/n;
				i = i % n;
				final int x = i%w;//w > h ? i / h : i % w;
				final int y = i/w;//w > h ? i % h : i / w;
				
				outRasters[z].setPixel(x, y, avg);
			}
		}
		
		logger.info("{} clusters processed",clustering.size());
		
		//new BufferedImage(image.getColorModel(), destRaster, true, null);
		
		for(int i = 0; i < l; i++){
			BufferedImage im = new BufferedImage(cms[i], outRasters[i], true, null);
			ImageIO.write(im, "jpg", new File(outDir,imFiles[i].getName()));
		}
		
		logger.info("visualizations saved");
		
		return 0;
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

}
