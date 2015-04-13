/**
 * 
 */
package util;

import java.awt.Point;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.PriorityQueue;

import javax.imageio.ImageIO;

import model.nb.CocentricPixelNeighborhood;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class MatrixMaker extends AbstractUtil {

	@Parameter(names = "-r")
	private double r = 10.0;
	
	@Parameter(names = "-k")
	private int k = 25;
	
	@Parameter(names = "-cielab")
	private boolean cielab = false;
	
	@Parameter(names = "-a")
	private double a = 1.0;
	
	@Parameter(names = "-b")
	private double b = 1.0;
	
	/* (non-Javadoc)
	 * @see util.AbstractUtil#run(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, boolean)
	 */
	@Override
	protected int run(Path input, Path output, boolean hdfsOutput)
			throws Exception {
		File infile = new File(input.toString());
		BufferedImage image = ImageIO.read(infile);
		if(cielab) image = CIELab.from(image);
		final Raster data = image.getRaster();
		final int w = image.getWidth();
		final int h = image.getHeight();
		final int n = w*h;
		
		//SequenceFile.Writer writer = null;
		BufferedWriter writer = null;
		final PriorityQueue<MatrixEntry> queue = new PriorityQueue<MatrixEntry>(k, new MatrixEntry.ValueComparator());
		final CocentricPixelNeighborhood nb = new CocentricPixelNeighborhood(r);
		final Map<Point,Double> dcache = new IdentityHashMap<Point, Double>(nb.size());
		
		for(Point p : nb){
			dcache.put(p, getDistMeasure(p,a));
		}
		
		double[] v1 = null;
		double[] v2 = null;
		
		long entries_written = 0;
		int min_column_size = Integer.MAX_VALUE;
		double avg_min_dval = 0.0;
		double avg_min_fval = 0.0;
		double avg_min_val = 0.0;
		double[] mins = new double[k];
		double[] maxs = new double[k];
		Arrays.fill(mins, 1.0);
		
		try {
			//Option pathOption = SequenceFile.Writer.file(output);
			//SequenceFile.createWriter(getConf(), pathOption);
			FileSystem fs = output.getFileSystem(getConf());
			writer = new BufferedWriter(new OutputStreamWriter(fs.create(output, true)));
			
			for(int y = 0; y < h; y++){
				for(int x = 0; x < w; x++){
					
					long idx1 = getIdx(x, y, w, h);
					v1 = data.getPixel(x, y, v1);
					double min_dval = 1.0;
					double min_fval = 1.0;
					float min = Float.MAX_VALUE;
					for(Point p : nb){
						
						if(x+p.x < 0 || y+p.y < 0 || y+p.y >= h || x+p.x >= w)
							continue;
						
						double dval = dcache.get(p);
						
						//add first k entries;
						if(queue.size() < k){
							double fval = getMeasure(v1, data.getPixel(x+p.x, y+p.y, v2), b);
							float val = (float) (dval * fval);
							if(val <= 0.0) continue;
							if(min > val) min = val;
							min_dval = Math.min(min_dval, dval);
							min_fval = Math.min(min_fval, fval);
							queue.add(MatrixEntry.get(idx1, getIdx(x+p.x, y+p.y, w, h), val));
							continue;
						}
						
						if(dval <= min) break;
						double fval = getMeasure(v1, data.getPixel(x+p.x, y+p.y, v2), b);
						float val = (float) (dval * fval);
						if(val <= min) continue;						
						queue.remove();
						min_dval = Math.min(min_dval, dval);
						min_fval = Math.min(min_fval, fval);
						queue.add(MatrixEntry.get(idx1, getIdx(x+p.x, y+p.y, w, h), val));
						min = queue.peek().val;
					}
					
					avg_min_dval += min_dval;
					avg_min_fval += min_fval;
					avg_min_val += min;
					
					if(min_column_size > queue.size()) min_column_size = queue.size();
					entries_written += queue.size();
					
					int i = queue.size();
					
					for(MatrixEntry e = queue.poll(); e != null; e = queue.poll()){
						--i;
						mins[i] = Math.min((double) e.val, mins[i]);
						maxs[i] = Math.max((double) e.val, maxs[i]);
						writer.append(String.valueOf(e.col))
						.append('\t')
						.append(String.valueOf(e.row))
						.append('\t')
						.append(String.valueOf(e.val))
						.append('\n');
					}
					
					queue.clear();
				}
			}
			
		} finally {
			if(writer != null) writer.close();
		}
		avg_min_val /= n;
		avg_min_dval /= n;
		avg_min_fval /= n;
		System.out.printf("nnz: %d, kavg: %4.1f, kmin: %d\n avg_min_dval: %f, avg_min_fval: %f, avg_min_val: %f\n", entries_written, (double) entries_written/n, min_column_size,avg_min_dval,avg_min_fval,avg_min_val);
		
		System.out.println("\nmin/max top values ----------");
		for(int i = 0; i < mins.length; i++){
			System.out.printf("%3d %e %e\n", i+1,mins[i],maxs[i]);
		}
		
		return 0;
	}
	
	private static double getDistMeasure(Point off, double a)
	{
		return Math.exp(-off.distanceSq(0.0, 0.0)/a);
	}
	
	public static double getMeasure(double[] p1, double[] p2){
		double val = distanceSq(p1, p2);
		return Math.exp(-val);
	}
	
	public static double getMeasure(double[] p1, double[] p2, double b){
		double val = distanceSq(p1, p2);
		return Math.exp(-val/b);
	}
	
	private static final long getIdx(int x, int y, int w, int h){
		return w < h ? x + w*y : y + h*x;
	}
	
	private static final double distanceSq(double[] v1, double[] v2){
		double sum = 0.0;
		for(int i = 0, end = v1.length; i < end; i++){
			sum += (v1[i]-v2[i])*(v1[i]-v2[i]);
		}
		return sum;
	}
	
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new MatrixMaker(), args));
	}
	
	private static final class MatrixEntry implements Comparable<MatrixEntry> {
		public long col;
		public long row;
		public float val;
		
		public MatrixEntry(){
			this(0,0,0.0f);
		}
		
		private MatrixEntry(long col, long row, float val){
			this.col = col;
			this.row = row;
			this.val = val;
		}
		
		public static MatrixEntry get(long col, long row, float val) {
			return new MatrixEntry(col,row,val);
		}
		
		@Override
		public int compareTo(MatrixEntry o) {
			int cmp = col == o.col ? 0 : col < o.col ? -1 : 1;
			if(cmp != 0) return cmp;
			return row == o.row ? 0 : row < o.row ? -1 : 1;
		}
		
		@Override
		public boolean equals(Object obj) {
			if(obj instanceof MatrixEntry){
				MatrixEntry o = (MatrixEntry) obj;
				return col == o.col && row == o.row && val == o.val;
			}
			return false;
		}
		
		@Override
		public int hashCode() {
			return (int) (31 * col + row);
		}
		
		@Override
		public String toString() {
			return String.format("[c: %d, r: %d, v: %f]", col,row,val);
		}
		
		public static class ValueComparator implements Comparator<MatrixEntry> {

			@Override
			public int compare(MatrixEntry o1, MatrixEntry o2) {
				return o1.val < o2.val ? -1 : o1.val > o2.val ? 1 : 0;
			}
			
		}
	}

}
