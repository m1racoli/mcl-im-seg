/**
 * 
 */
package io.writables;

import java.awt.Point;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author Cedrik
 *
 */
public final class TOFPixel implements FeatureWritable<TOFPixel>, Configurable {

	public static final String SIGMA_X_CONF = "sigma.x";
	public static final String SIGMA_I_CONF = "sigma.y";
	
	private final Point p = new Point();
	private final double[] X = new double[3]; // {X,Y,Z}
	private double I;
	
	private double sigma_I = 1.0;
	private double sigma_X = 1.0;
	
	@Override
	public float dist(TOFPixel o) {
		double d = 0.0;
		for(int i = 0; i < 3; i++){
			final double diff = x[i] - o.x[i];
			d += diff*diff;
		}
		d += norm(x-o.x,y-o.y);
		return (float) Math.exp(-d);
	}

	private static final double norm(double x, double y){
		return x*x + y*y;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, p.x);
		WritableUtils.writeVInt(out, p.y);
		for(int i = 0; i < 3; i++)
			out.writeDouble(x[i]);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		p.x = WritableUtils.readVInt(in);
		p.y = WritableUtils.readVInt(in);
		for(int i = 0; i < 4; i++)
			x[i] = in.readDouble();
	}
	
	@Override
	public String toString() {
		return String.format("p: %s, %f, (%f,%f,%f)", p, I, X[0], X[1], X[2]);
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

}
