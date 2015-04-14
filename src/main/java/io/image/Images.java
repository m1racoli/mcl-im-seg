/**
 * 
 */
package io.image;

import java.awt.Point;
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
import java.util.Collection;

/**
 * utility functions for images
 * 
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
	
	public static ColorModel createGrayScaleColorModel(){
		ColorSpace cs = ColorSpace.getInstance(ColorSpace.CS_GRAY);
	    int[] nBits = { 8 };
	    return new ComponentColorModel(cs, nBits, false, true,
	            Transparency.OPAQUE, DataBuffer.TYPE_BYTE);
	}
	
	/**
	 * @param vals values in range [0.0,1.0]
	 * @param w width
	 * @param h height
	 * @return BufferedImage
	 */
	public static BufferedImage createGrayScaleImage(double[] vals, int w, int h) {
		if(w * h != vals.length) throw new IllegalArgumentException("val dim does not match image dim");
		
		final byte[] buffer = new byte[vals.length];
		for (int i = buffer.length - 1; i >= 0; --i) {
	    	buffer[i] = (byte) (255.0 * vals[i]);
	    }
		
	    ColorModel cm = createGrayScaleColorModel();
	    SampleModel sm = cm.createCompatibleSampleModel(w, h);
	    DataBufferByte db = new DataBufferByte(buffer, w * h);
	    WritableRaster raster = Raster.createWritableRaster(sm, db, null);
	    return new BufferedImage(cm, raster, false, null);
	}
	
}
