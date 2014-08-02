/**
 * 
 */
package util;

import java.awt.Point;
import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;

import model.nb.RadialPixelNeighborhood;

/**
 * @author Cedrik
 *
 */
public class ConnectivityGraph {
	
	public static BufferedImage visualize(RenderedImage image, RadialPixelNeighborhood nb, final double scale){
		
		if(nb == null || image == null){
			return null;
		}
		
		final int w = image.getWidth();
		final int h = image.getHeight();
		final Raster raster = image.getData();
		float[] p1 = null;
		float[] p2 = null;
		
		byte[] result = new byte[h*w];
		
		for(int y1 = 0; y1 < h; y1++){
			for(int x1 = 0; x1 < w; x1++){
				double d = 0.0f;
				int n = 0;
				for(Point off : nb){
					final int x2 = x1+off.x;
					final int y2 = y1+off.y;
					
					if(x2 < 0 || y2 < 0 || x2 >= w || y2 >= h)
						continue;
					
					d += metric(raster.getPixel(x1, y1, p1), raster.getPixel(x2, y2, p2));
					n++;
				}
				double avg = d/n;
				double val = Math.exp(-avg*avg/scale);
				result[x1 + w*y1] = (byte) (255.0*val);
			}
		}
		
		ColorSpace cs = ColorSpace.getInstance(ColorSpace.CS_GRAY);
		ColorModel cm = new ComponentColorModel(cs, new int[]{8}, false, true, Transparency.OPAQUE, DataBuffer.TYPE_BYTE);
		SampleModel sm = cm.createCompatibleSampleModel(w, h);
		DataBuffer db = new DataBufferByte(result, h*w);
		WritableRaster wr = WritableRaster.createWritableRaster(sm, db, null);
		BufferedImage resultImage = new BufferedImage(cm, wr, true, null);
		return resultImage;
	}
	
	public static final double metric(float[] v1, float[] v2){
		double sum = 0.0f;
		
		for(int i = 0; i < v1.length; i++){
			sum += (v1[i]-v2[i])*(v1[i]-v2[i]);
		}
		
		return Math.sqrt(sum);
	}
	
}
