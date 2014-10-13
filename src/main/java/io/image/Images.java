/**
 * 
 */
package io.image;

import java.awt.Point;
import java.awt.image.Raster;
import java.util.Collection;

/**
 * @author Cedrik
 *
 */
public class Images {

	public static double[] getAvgPixel(Raster raster, Collection<Point> points) {
		double[] avg = null;
		double[] add = null;
		for(Point p : points){
			if(avg == null){
				avg = raster.getPixel(p.x, p.y, avg);
			} else {
				add = raster.getPixel(p.x, p.y, add);
				addToFirst(avg,add);
			}
		}
		div(avg,points.size());
		return avg;
	}
	
	
	
	public static double sumDiff(Raster raster, Collection<Point> points, double[] center) {
		double avg = 0.0;
		double[] v = null;
		for(Point p : points) {
			avg += dist(center,raster.getPixel(p.x, p.y, v));
		}
		return avg;
	}
	
	public static final void addToFirst(double[] v1, double[] v2){
		for(int j = 0; j < 3; j++)
			v1[j] += v2[j];
	}
	
	public static final double distSq(double[] v1, double[] v2){
		double s = 0.0;
		for(int i = 2; i >= 0;--i)
			s += (v1[i]-v2[i]) * (v1[i]-v2[i]);
		return s;
	}
	
	public static final double dist(double[] v1, double[] v2){
		return Math.sqrt(distSq(v1, v2));
	}
	
	public static final double norm(double[] v){
		double sum = 0.0;
		for(int i = 2; i >= 0;--i){
			sum += v[i]*v[i];
		}
		return Math.sqrt(sum);
	}
	
	public static void div(double[] v, double d){
		for(int i = 2; i >= 0; --i)
			v[i] /= d;
	}
	
	public static void add(double[] v, double d){
		for(int i = 2; i >= 0; --i)
			v[i] += d;
	}
	
	public static void bound(double[] v) {
		for(int i = 2; i >= 0; --i)
			v[i] = Math.max(0.0, Math.min(255.0, v[i]));		
	}
	
}
