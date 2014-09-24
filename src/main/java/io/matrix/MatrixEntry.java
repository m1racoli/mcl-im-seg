package io.matrix;

import java.util.Comparator;

import mapred.MCLInstance;

public final class MatrixEntry implements Comparable<MatrixEntry> {
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