package io.heap;

public final class HeapItem {

	private final Object data;
	private HeapItem child = null;
	private HeapItem left;
	private HeapItem right;
	private int degree = 0;
	
	public HeapItem(Comparable<?> data, HeapItem nb) {
		this.data = data;
		
		if(nb == null){
			left = this;
			right = this;
		} else {
			right = nb;
			left = nb.left;
			nb.left = this;
			left.right = this;				
		}
	}
	
	public int degree() {
		return degree;
	}

	/**
	 * remove node from its siblings
	 */
	public void decouple(){
		left.right = right;
		right.left = left;
	}
	
	public void link(HeapItem parent){
		left.right = right;
		right.left = left;
		
		if(parent.child == null){
			parent.child = this;
			right = this;
			left = this;
		} else {
			left = parent.child;
			right = parent.child.right;
			parent.child.right = this;
			right.left = this;				
		}
	}
	
	public boolean hasChild(){
		return child != null;
	}
	
	public HeapItem child(){
		return child;
	}
	
	public HeapItem right(){
		return right;
	}
	
	public void takeDownChildren(){
		HeapItem min_left = left;
		HeapItem z_child_left = child.left;
		left = z_child_left;
		z_child_left.right = this;
		child.left = min_left;
		min_left.right = child;
	}
	
	public Object data(){
		return data;
	}
	
	public boolean hasSiblings(){
		return this != right;
	}
}