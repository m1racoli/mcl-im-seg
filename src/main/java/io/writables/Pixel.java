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
public final class Pixel implements SpatialFeatureWritable<Pixel>, Configurable {

	public static final String SIGMA_X_CONF = "sigma.X";
	public static final String SIGMA_F_CONF = "sigma.F";
	
	private final Point p = new Point();
	private final double[] F = new double[3];
	
	private double sigma_F = 1.0;
	private double sigma_X = 1.0;
	
	@Override
	public float dist(Pixel o) {
		double dX = p.distanceSq(o.p)/sigma_X;
		double dF = distSq(F, o.F)/sigma_F;
		return (float) Math.exp(-dX-dF);
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
			out.writeDouble(F[i]);
		}
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		p.x = WritableUtils.readVInt(in);
		p.y = WritableUtils.readVInt(in);
		
		for(int i = 0; i < 3; i++){
			F[i] = in.readDouble();
		}
		
	}
	
	public void setX(int x){
		p.x = x;
	}
	
	public void setY(int y){
		p.y = y;
	}
	
	public double[] getF(){
		return F;
	}
	
	@Override
	public String toString() {
		return String.format("p: %s, {%f,%f,%f}", p,F[0],F[1],F[2]);
	}

	@Override
	public Pixel copy(Pixel instance) {

		if(instance == null){
			instance = new Pixel();
		}
		
		instance.p.setLocation(p);
		System.arraycopy(F, 0, instance.F, 0, F.length);
		return instance;
	}

	@Override
	public Point getPosition() {
		return p;		
	}

	@Override
	public void setConf(Configuration conf) {
		sigma_F = conf.getDouble(SIGMA_F_CONF, sigma_F);
		sigma_X = conf.getDouble(SIGMA_X_CONF, sigma_X);
	}

	@Override
	public Configuration getConf() {
		Configuration conf = new Configuration();
		conf.setDouble(SIGMA_F_CONF, sigma_F);
		conf.setDouble(SIGMA_X_CONF, sigma_X);
		return conf;
	}

}
