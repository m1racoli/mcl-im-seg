/**
 * 
 */
package io.cluster;

import iterators.IteratorView;

import java.awt.Point;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.AbstractSet;
import java.util.Iterator;

import transform.IntTo2D;

/**
 * @author Cedrik
 *
 */
public class DefaultImageClustering extends AbstractSet<ImageCluster>
		implements ImageClustering {

	private final IndexedClusteringView<Point> inst;
	private final int h;
	private final int w;
	
	public DefaultImageClustering(File file, int w, int h) throws IOException {
		inst = new IndexedClusteringView<Point>(new ArrayClustering(file), new IntTo2D(h));
		this.w = w;
		this.h = h;
	}
	
	public DefaultImageClustering(Reader reader, int w, int h) throws IOException {
		inst = new IndexedClusteringView<Point>(new ArrayClustering(reader), new IntTo2D(h));
		this.w = w;
		this.h = h;
	}
	
	@Override
	public Iterator<ImageCluster> iterator() {
		return new IteratorView<Cluster<Point>, ImageCluster>(inst.iterator()) {

			@Override
			public ImageCluster get(Cluster<Point> val) {
				return new DefaultImageCluster(val);
			}
		};
	}

	@Override
	public int size() {
		return inst.size();
	}
	
	@Override
	public ImageCluster getCluster(Point e) {
		return new DefaultImageCluster(inst.getCluster(e));
	}
	
	private class DefaultImageCluster extends AbstractSet<Point> implements ImageCluster {

		private final Cluster<Point> inst;
		
		private DefaultImageCluster(Cluster<Point> inst) {
			this.inst = inst;
		}
		
		@Override
		public Iterator<Point> iterator() {
			return inst.iterator();
		}

		@Override
		public int size() {
			return inst.size();
		}

		@Override
		public boolean isBoundary(Point p) {
			return getNeighbouringCluster(p) == null;
		}
		
		@Override
		public ImageCluster getNeighbouringCluster(Point p) {
			
			if(!contains(p))
				return null;
			
			Point tmp = new Point();
			
			tmp.setLocation(p.x+1, p.y);
			if(inBounds(tmp) && !contains(tmp)) return getCluster(tmp);
			tmp.setLocation(p.x-1, p.y);
			if(inBounds(tmp) && !contains(tmp)) return getCluster(tmp);
			tmp.setLocation(p.x, p.y+1);
			if(inBounds(tmp) && !contains(tmp)) return getCluster(tmp);
			tmp.setLocation(p.x, p.y-1);
			if(inBounds(tmp) && !contains(tmp)) return getCluster(tmp);
			return null;
		}
		
		private boolean inBounds(Point p){
			return p.x >= 0 && p.y >= 0 && p.x < w && p.y < h;
		}

		@Override
		public ImageCluster not() {
			return new DefaultImageCluster(inst.not());
		}
		
	}
	
	
	
}
