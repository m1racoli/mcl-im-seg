package io.writables;

public final class SliceEntry implements Comparable<SliceEntry> {
	public int col;
	public long row;
	public float val;
	
	public SliceEntry(){
		this(0,0,0.0f);
	}
	
	private SliceEntry(int col, long row, float val){
		this.col = col;
		this.row = row;
		this.val = val;
	}
	
	public static SliceEntry get(int col, long row, float val) {
		return new SliceEntry(col,row,val);
	}
	
	@Override
	public int compareTo(SliceEntry o) {
		int cmp = col == o.col ? 0 : col < o.col ? -1 : 1;
		if(cmp != 0) return cmp;
		return row == o.row ? 0 : row < o.row ? -1 : 1;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SliceEntry){
			SliceEntry o = (SliceEntry) obj;
			return col == o.col && row == o.row && val == o.val;
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return 31 * col + (int) row;
	}
	
	@Override
	public String toString() {
		return String.format("[c: %d, r: %d, v: %f]", col,row,val);
	}
}