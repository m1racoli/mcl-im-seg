/**
 * 
 */
package io.cluster;

import io.image.Images;

import java.awt.Point;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import util.CIELab;

/**
 * utility functions for image clusterings
 * 
 * @author Cedrik
 *
 */
public class ImageClusterings extends Clusterings {

	public static ImageClustering read(File file, int w, int h) throws IOException {
		return new DefaultImageClustering(file, w, h);
	}
	
	@SuppressWarnings("unused")
	public static void testInverse(ImageClustering clustering){
		Integer total = null;
		for(ImageCluster cluster : clustering){
			int size = cluster.size();
			int cnt = 0;
			for(Object e : cluster){
				cnt++;
			}
			if(cnt != size){
				System.err.printf("size != cnt: %d != %d\n",size,cnt);
			}
			
			ImageCluster inv = cluster.not();
			int inv_size = inv.size();
			int inv_cnt = 0;
			for(Object e : inv){
				inv_cnt++;
			}
			if(inv_cnt != inv_size){
				System.err.printf("inv_size != inv_cnt: %d != %d\n",inv_size,inv_cnt);
			}
			
			if(total == null){
				total = size + inv_size;
			} else {
				if(total.compareTo(size + inv_size) != 0){
					System.err.printf("total inconsistent: %d != %d\n",total,size + inv_size);
				}
			}
		}
	}
	
	public static BufferedImage visualize(ImageClustering clustering, BufferedImage image) {
		final Raster raster = CIELab.from(image).getData();
		final WritableRaster destRaster = raster.createCompatibleWritableRaster();
		final Map<ImageCluster,double[]> center = new HashMap<ImageCluster,double[]>(clustering.size());
		final Map<ImageCluster,Double> avgDiff = new HashMap<ImageCluster,Double>(clustering.size());
		double max_diff = 0.0;
		final double[] val = new double[3];
		
		for(ImageCluster cluster : clustering){
			double[] avg = Images.getAvgPixel(raster, cluster);
			center.put(cluster, avg);
			double diff = Images.sumDiff(raster, cluster, avg);
			diff /= cluster.size();
			avgDiff.put(cluster, diff);
			max_diff = Math.max(max_diff, diff);
		}
		
		System.out.printf("max avg diff: %f\n",max_diff);
		
		for(ImageCluster cluster : clustering){
			//double v = 255.0 * avgDiff.get(cluster)/max_diff;
			double[] c = center.get(cluster);
			
			for(Point p : cluster){
				ImageCluster nb = cluster.getNeighbouringCluster(p);
				if(nb != null){
					double d = Images.dist(c, center.get(nb));
					Arrays.fill(val, d*10);		
				} else {
					Arrays.fill(val, 0.0);
					//Arrays.fill(val, (int) v);
				}
				//Arrays.fill(val, (int) v);
				destRaster.setPixel(p.x, p.y, val);
			}
		}
		
		return new BufferedImage(image.getColorModel(), destRaster, true, null);		
	}
	
	public static double nCut(ImageClustering clustering, final Raster raster,int te){
		ExecutorService executorService = Executors.newFixedThreadPool(te);
		List<Future<Double>> results = new ArrayList<Future<Double>>(clustering.size());
		
		for(final ImageCluster cl : clustering){
			results.add(executorService.submit(new Callable<Double>() {

				@Override
				public Double call() throws Exception {
					return nassoc(cl, raster);
				}
			}));
		}
		
		double ncut = 0.0;
		for(Future<Double> result : results){
			try {
				ncut += result.get();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}	
		
		return clustering.size() - ncut;
	}
	
	public static double nCut(ImageClustering clustering, Raster raster){
		double ncut = 0.0;
		for(ImageCluster cl : clustering){
//			ncut += nCut(cl, raster);
			ncut += nassoc(cl, raster);
		}
		return clustering.size() - ncut;
	}
	
	public static double nassoc(ImageCluster cl, Raster raster){
		double[] v1 = new double[3];
		double[] v2 = new double[3];
		return cut(cl, cl, raster, v1, v2)/cut(cl, raster, v1, v2);
	}
	
	public static double nCut(ImageCluster cluster, Raster raster){
		double[] v1 = new double[3];
		double[] v2 = new double[3];
		return cut(cluster, cluster.not(), raster, v1, v2)/cut(cluster, raster, v1, v2);
	}
	
	public static double cut(ImageCluster cl, Raster raster, double[] v1, double[] v2){
		double cut = 0.0;
		for(Point p : cl){
			cut += cut(p, raster, v1, v2);
		}
		return cut;
	}
	
	public static double cut(ImageCluster cl1, ImageCluster cl2, Raster raster, double[] v1, double[] v2){
		//System.err.println("do "+cl1+" and "+cl2);
		double cut = 0.0;
		for(Point p : cl2){
			cut += cut(p, cl1, raster, v1, v2);
		}
		return cut;
	}
	
	public static double cut(Point p, Raster raster, double[] v1, double[] v2){
		double cut = 0.0;
		v1 = raster.getPixel(p.x, p.y, v1);
		
		for(int y = 0, h = raster.getHeight(); y < h; y++){
			for(int x = 0, w = raster.getWidth(); x < w; x++){
				v2 = raster.getPixel(x, y, v2);
				cut += getMeasure(v1, v2);
			}			
		}

		return cut;
	}
	
	public static double cut(Point p, ImageCluster cl, Raster raster, double[] v1, double[] v2){
		double cut = 0.0;
		v1 = raster.getPixel(p.x, p.y, v1);
		
		for(Point p2 : cl){
			v2 = raster.getPixel(p2.x, p2.y, v2);
			cut += getMeasure(v1, v2);
		}

		return cut;
	}
	
	public static double getMeasure(double[] p1, double[] p2){
		double val = distanceSq(p1, p2);
		return Math.exp(-val);
	}
	
	public static double getMeasure(double[] p1, double[] p2, double b){
		double val = distanceSq(p1, p2);
		return Math.exp(-val/b);
	}
	
	private static final double distanceSq(double[] v1, double[] v2){
		double sum = 0.0;
		for(int i = 0, end = v1.length; i < end; i++){
			sum += (v1[i]-v2[i])*(v1[i]-v2[i]);
		}
		return sum;
	}
	
}
