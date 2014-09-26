/**
 * 
 */
package io.cluster;

import java.awt.Point;

/**
 * @author Cedrik
 *
 */
public class P2dClusterSet extends ClusterSetView<Integer, Point> {

	private final int h;
	
	public P2dClusterSet(int h, ClusterSet<Integer> instance) {
		super(instance);
		this.h = h;
	}

	@Override
	public Point forw(Integer src) {
		return new Point(src / h, src % h);
	}

	@Override
	public Integer back(Object dest) {
		if(dest instanceof Point){
			Point p = (Point) dest;
			return p.x*h + p.y;
		}
		return null;
	}

}
