package io.writables;

import io.writables.MCLMatrixSlice.MatrixEntry;

import java.util.Random;

import mapred.MCLConfigHelper;
import mapred.MCLContext;
import mapred.PrintMatrix;

import org.apache.commons.math3.linear.DefaultRealMatrixPreservingVisitor;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealMatrixPreservingVisitor;
import org.apache.hadoop.conf.Configuration;

public class MCLSliceTest {
	
	private static final long n = 4;
	private static final int nsub = 4;
	private static final int k = 4;
	
	public static void main(String[] args) {
		
		Configuration conf = getConf();		
		OpenMapRealMatrix m1 = getTestMatrix((int) n, nsub, k);
		OpenMapRealMatrix m2 = getTestMatrix((int) n, nsub, k);
		CSCSlice s1 = MCLSliceTest.<CSCSlice>asMatrixSlice(m1, conf);
		CSCSlice s2 = MCLSliceTest.<CSCSlice>asMatrixSlice(m2, conf);
		OpenMapRealMatrix mr = m2.multiply(m1);
		CSCSlice sr = s1.multipliedBy(s2, null);
		System.out.println("dist after mult:"+dist(mr, sr));
		mr = mr.add(m2);
		sr.add(s2);
		System.out.println("dist after add: "+dist(mr, sr));
		System.out.println(sr);
		int k = sr.inflateAndPrune(null);
		System.out.println(sr);
		System.out.println(k);
	}
	
	public static final Configuration getConf() {
		Configuration conf = new Configuration();
		MCLConfigHelper.setN(conf, n);
		MCLConfigHelper.setNSub(conf, nsub);
		MCLConfigHelper.setKMax(conf, k);
		MCLConfigHelper.setPrintMatrix(conf, PrintMatrix.ALL);
		MCLConfigHelper.setDebug(conf, true);
		MCLConfigHelper.setSelection(conf, 2);
		return conf;
	}
	
	public static final <M extends MCLMatrixSlice<M>> M asMatrixSlice(RealMatrix m, Configuration conf) {
		M slice = MCLContext.<M>getMatrixSliceInstance(conf);
		int nsub = MCLConfigHelper.getNSub(conf);
		int k = MCLConfigHelper.getKMax(conf);
		int nnz = k * nsub;
		final int[] cols = new int[nnz];
		final long[] rows = new long[nnz];
		final float[] vals = new float[nnz];
		
		RealMatrixPreservingVisitor visitor = new DefaultRealMatrixPreservingVisitor() {
			int i = 0;
			@Override
			public void visit(int row, int column, double value) {
				cols[i] = column; rows[i] = row; vals[i++] = (float) value;
			}
		};
		m.walkInColumnOrder(visitor);
		slice.fill(cols, rows, vals);
		return slice;
	}
	
	public static final OpenMapRealMatrix getTestMatrix(int n, int nsub, int k) {
		OpenMapRealMatrix m = new OpenMapRealMatrix(n, nsub);		
		Random rand = new Random();		
		for (int j = 0; j < nsub; j++) {
			float sum = 0;
			for (int i = 0; i < k; i++) {
				int p = rand.nextInt(n);
				while(m.getEntry(p, j) != 0.0) {
					p = (p + 1) % n;
				}
				float val = (float) rand.nextDouble();
				sum += val;
				m.setEntry(p, j, val);
			}
			
			for(int i = 0; i < n; i++) {
				if(m.getEntry(i, j) != 0.0) {
					m.setEntry(i, j, m.getEntry(i, j)/sum);
				}
			}
		}
		return m;
	}
	
	public static final <M extends MCLMatrixSlice<M>> double dist(OpenMapRealMatrix o, M m) {
		OpenMapRealMatrix o2 = o.createMatrix(o.getRowDimension(), o.getColumnDimension());
		
		for(MatrixEntry e : m.dump()) {
			o2.setEntry((int) e.row, e.col, e.val);
		}
		
		return o.subtract(o2).getNorm();
	}
}
