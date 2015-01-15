package model.nb;

import java.awt.Dimension;
import java.awt.Point;
import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class CocentricPixelNeighborhood implements Iterable<Point> {

	private final List<Point> points;
	
	public CocentricPixelNeighborhood(double radius){
		
		final int ceil_r = (int) Math.ceil(radius);
		
		List<Point> list = new ArrayList<Point>(ceil_r*ceil_r);
		for(int x = - ceil_r; x <= ceil_r; x++){
			for(int y = - ceil_r; y <= ceil_r; y++){
				if(Math.sqrt(x*x +y*y) <= radius){
					list.add(new Point(x, y));
				}
			}
		}
		
		Collections.sort(list, new Comparator<Point>(){

			@Override
			public int compare(Point o1, Point o2) {
				double d1 = o1.distanceSq(0.0, 0.0);
				double d2 = o2.distanceSq(0.0, 0.0);
				return d1 < d2 ? -1 : d1 > d2 ? 1 : 0;
			}
			
		});
		
		points = new ArrayList<Point>(list);
	}
	
	public List<Point> local(int x, int y, int w, int h, List<Point> list){
		
		list.clear();
		for(Point p : points){
			final int x2 = p.x;
			final int y2 = p.y;
			if(x2 < 0 || y2 < 0 || x2 >= w || y2 >= h)
				continue;
			list.add(p);
		}
		
		return list;
	}
	
	@Override
	public Iterator<Point> iterator() {
		return points.iterator();
	}
	
	public int size(){
		return points.size();
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
