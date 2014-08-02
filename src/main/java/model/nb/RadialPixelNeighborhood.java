package model.nb;

import java.awt.Dimension;
import java.awt.Point;
import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RadialPixelNeighborhood implements Iterable<Point> {

	private final int[] x_off;
	private final int[] y_off;
	
	public RadialPixelNeighborhood(double radius){
		
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
	
}