/**
 * 
 */
package io.writables;

import iterators.EmptyIterator;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * @author Cedrik
 *
 */
public class FibonacciHeap<E extends Comparable<E>> extends AbstractQueue<E> {

	private final int maxRank;
	private final int maxSize;
	private int inserted = 0;
	private HeapItem root = null;
	
	public FibonacciHeap(int size) {
		if(size <= 0) throw new IllegalArgumentException("heap size must be positive! "+size);
		this.maxSize = size;
		this.maxRank = maxRank(size);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean offer(E e) {
		if(root == null){
			root = new HeapItem(e, null);
			inserted = 1;
			return true;
		}
		
		if(maxSize == inserted){			
			if(e.compareTo((E) root.data()) <= 0){
				return false;
			}			
			poll();
		}
		
		HeapItem item = new HeapItem(e, root);
		inserted++;
		
		if(compare(item, root) < 0){
			root = item;
		}
		
		return true;
	}
	
	@SuppressWarnings("unchecked")
	private final int compare(HeapItem e1, HeapItem e2){
		return ((E) e1.data()).compareTo((E) e2.data());
	}

	@SuppressWarnings("unchecked")
	@Override
	public E poll() {
		HeapItem z = root;
		
		if(z == null){
			return null;
		}
		
		if(z.hasChild()){
			z.takeDownChildren();
		}
		
		if(!z.hasSiblings()){
			root = null;
		} else {
			z.decouple();
			root = z.right();
			consolidate();
		}
		
		inserted--;
		
		return (E) z.data;
	}

	private final void consolidate() {

		HeapItem[] arr = new HeapItem[maxRank];
		
		HeapItem s = root;
		HeapItem w = root;
		
		do {
			HeapItem x = w;
			HeapItem nextW = w.right();
			int d = x.degree();
			
			while(arr[d] != null){
				HeapItem y = arr[d];
				
				if(compare(x, y) > 0){
					HeapItem tmp = y;
					y = x;
					x = tmp;
				}
				
				if(y == s){
					s = s.right();
				}
				
				if(y == nextW){
					nextW = nextW.right();
				}
				
				y.link(x);
				arr[d] = null;
				d++;
			}
			
			arr[d] = x;
			w = nextW;
		} while (w != s);
		
		root = s;
		
		for(HeapItem i : arr){
			if(i != null && compare(i, root) < 0){
				root = i;
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public E peek() {
		return root == null ? null : (E) root.data();
	}

	@Override
	public Iterator<E> iterator() {
		if(root == null)
			return new EmptyIterator<E>();
		
		LinkedList<E> list = new LinkedList<E>();
		getItems(list, root);		
		HeapItem next = root.right();
		
		while(next != root){
			getItems(list, next);
			next = next.right();
		}		
		
		return list.iterator();
	}
	
	@SuppressWarnings("unchecked")
	private final LinkedList<E> getItems(LinkedList<E> list, HeapItem item){
		list.add((E) item.data());
		
		if(item.hasChild()){
			HeapItem child = item.child();
			getItems(list, child);			
			HeapItem next = child.right();
			
			while(next != child){
				getItems(list, next);
				next = next.right();
			}
		}
		
		return list;			
	}

	@Override
	public int size() {
		return inserted;
	}

	@Override
	public void clear() {
		inserted = 0;
		root = null;
	}

	private static final int maxRank(int size){
		return (int) Math.floor(Math.log(size)/Math.log((1.0 + Math.sqrt(5.0))/2.0));
	}

	public static final class HeapItem {
	
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

}
