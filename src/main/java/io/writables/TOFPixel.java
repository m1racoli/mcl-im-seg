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
public final class TOFPixel implements SpatialFeatureWritable<TOFPixel>, Configurable {

	public static final String SIGMA_X_CONF = "sigma.X";
	public static final String SIGMA_I_CONF = "sigma.I";
	
	private final Point p = new Point();
	private final double[] X = new double[3]; // {X,Y,Z}
	private double I;
	
	private double sigma_I = 1.0;
	private double sigma_X = 1.0;
	
	public void setP(int x, int y){
		p.setLocation(x, y);
	}
	
	public void setP(Point p){
		p.setLocation(p);
	}
	
	public void setX(double x){
		X[0] = x;
	}
	
	public void setY(double y){
		X[1] = y;
	}
	
	public void setZ(double z){
		X[2] = z;
	}
	
	public void setI(double i){
		I = i;
	}
	
	@Override
	public float dist(TOFPixel o) {
		//double dX = p.distanceSq(o.p)/sigma_X;
		double dX = distSq(X, o.X)/sigma_X;
		double dI = (I - o.I) * (I - o.I)/sigma_I;
		return (float) Math.exp(-(dX+dI));
	}

	private static final double distSq(double[] v1, double[] v2){
		double s = 0.0;
		for(int i = 2; i >= 0; i--){
			s += (v1[i] - v2[i])*(v1[i] - v2[i]);
		}
		return s;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, p.x);
		WritableUtils.writeVInt(out, p.y);
		
		for(int i = 0; i < 3; i++){
			out.writeDouble(X[i]);
		}
		
		out.writeDouble(I);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		p.x = WritableUtils.readVInt(in);
		p.y = WritableUtils.readVInt(in);
		
		for(int i = 0; i < 3; i++){
			X[i] = in.readDouble();
		}
		
		I = in.readDouble();
	}
	
	@Override
	public String toString() {
		return String.format("p: %s, %f, (%f,%f,%f)", p, I, X[0], X[1], X[2]);
	}

	@Override
	public void setConf(Configuration conf) {
		sigma_I = conf.getDouble(SIGMA_I_CONF, sigma_I);
		sigma_X = conf.getDouble(SIGMA_X_CONF, sigma_X);
	}

	@Override
	public Configuration getConf() {
		Configuration conf = new Configuration();
		conf.setDouble(SIGMA_I_CONF, sigma_I);
		conf.setDouble(SIGMA_X_CONF, sigma_X);
		return conf;
	}

	@Override
	public Point getPosition() {
		return p;
	}

}
