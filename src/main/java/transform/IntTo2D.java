/**
 * 
 */
package transform;

import java.awt.Point;

/**
 * @author Cedrik
 *
 */
public class IntTo2D implements Transformation<Integer, Point> {

	private final int h;
	
	public IntTo2D(int h) {
		this.h = h;
	}

	@Override
	public Point get(Integer val) {
		return new Point(val / h, val % h);
	}

	@Override
	public Integer inv(Object val) {
		if(val instanceof Point){
			Point p = (Point) val;
			return p.x*h + p.y;
		}
		return null;
	}

}
