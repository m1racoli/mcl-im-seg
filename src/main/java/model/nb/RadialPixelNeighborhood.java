package model.nb;

import java.awt.Dimension;
import java.awt.Point;
import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * a model of a radial pixel neighborhood
 * 
 * @author Cedrik
 *
 */
public class RadialPixelNeighborhood implements Iterable<Point> {

	private final int[] x_off;
	private final int[] y_off;
	private final double radius;
	
	public RadialPixelNeighborhood(double radius){
		this.radius = radius;
		final int ceil_r = (int) Math.ceil(radius);
		
		List<Point> points = new ArrayList<Point>(ceil_r*ceil_r);
		for(int x = - ceil_r; x <= ceil_r; x++){
			for(int y = - ceil_r; y <= ceil_r; y++){
				if(Math.sqrt(x*x +y*y) <= radius){
					points.add(new Point(x, y));
				}
			}
		}
		
		final int size = points.size();
		x_off = new int[size];
		y_off = new int[size];
		
		for(int i = 0; i< size;i++){
			Point p = points.get(i);
			x_off[i] = p.x;
			y_off[i] = p.y;
		}
	}
	
	public double getRadius() {
		return radius;
	}
	
	public List<Point> local(int x, int y, int w, int h, List<Point> list){
		
		list.clear();
		for(int i = 0; i  < x_off.length; i++){
			final int x2 = x + x_off[i];
			final int y2 = y + y_off[i];
			if(x2 < 0 || y2 < 0 || x2 >= w || y2 >= h)
				continue;
			list.add(new Point(x2, y2));
		}
		
		return list;
	}
	
	public List<Point> local(Point p, int w, int h, List<Point> list){
		return local(p.x, p.y, w, h, list);
	}
	
	@Override
	public Iterator<Point> iterator() {
		return new Iterator<Point>() {

			private final Point p = new Point();
			private int i = 0;
			
			@Override
			public boolean hasNext() {
				return i < x_off.length;
			}

			@Override
			public Point next() {
				p.setLocation(x_off[i], y_off[i++]);
				return p;
			}

			@Override
			public void remove() {
				new IllegalArgumentException("removing is not supported");	
			}
		};
	}
	
	public int size(){
		return x_off.length;
	}
	
	public static Rectangle getValidRect(Point off, Dimension dim){
		return new Rectangle(
				Math.max(0, - off.x),
				Math.max(0, - off.y),
				dim.width - Math.abs(off.x),
				dim.height - Math.abs(off.y));
	}
	
	public static int size(double r) {
		
		double RS = r*r; 	
		int cnt = 0;
		
		for(int R = (int) Math.floor(r), x = -R; x <= R; x++){
			double XS = x*x;
			for(int y = -R; y <= R; y++){
				if(y*y + XS <= RS){
					cnt++;
				}
			}
		}
		
		return cnt;
	}
	
}
