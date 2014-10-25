/**
 * 
 */
package io.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author Cedrik
 *
 */
public final class Pixel implements FeatureWritable<Pixel> {

	public int x;
	public int y;
	public final int[] v = new int[3];
	
	@Override
	public float dist(Pixel o) {
		double d = 0.0;
		for(int i = 0; i < 3; i++){
			final double diff = v[i] - o.v[i];
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
		WritableUtils.writeVInt(out, x);
		WritableUtils.writeVInt(out, y);
		for(int i = 0; i < 3; i++)
			out.writeByte(v[i]);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x = WritableUtils.readVInt(in);
		y = WritableUtils.readVInt(in);
		for(int i = 0; i < 3; i++)
			v[i] = in.readUnsignedByte();
	}
	
	@Override
	public String toString() {
		return String.format("x: %d, y: %d, {%d,%d,%d}", x,y,v[0],v[1],v[2]);
	}

	@Override
	public Pixel copy(Pixel instance) {

		if(instance == null){
			instance = new Pixel();
		}
		
		instance.x = x;
		instance.y = y;
		System.arraycopy(v, 0, instance.v, 0, v.length);
		return instance;
	}

}
