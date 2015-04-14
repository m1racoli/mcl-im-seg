/**
 * 
 */
package io.writables;

import io.heap.FibonacciHeap;

import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Queue;

import mapred.MCLStats;

/**
 * @author Cedrik
 *
 */
public abstract class FloatMatrixSlice<M extends FloatMatrixSlice<M>> extends MCLMatrixSlice<M> {

	private Queue<QueueItem> queue = null;
	
	protected final void inflate(float[] val, int s, int t) {
		final double I = inflation;
		float sum = 0.0f;
		
		for(int i = s; i < t; i++) {
			final float inf_val = (float) Math.pow(val[i], I);
			val[i] = inf_val;
			sum += val[i];
		}
		
		for(int i = s; i < t; i++) {
			val[i] /= sum;
		}
	}
	
	protected static final int threshPrune(float[] val, int s, int t, int[] selection, MCLStats stats, float thresh) {
		int selected = 0;
		for(int i = s; i < t; i++){
			if(val[i] >= thresh){
				selection[selected++] = i;
			} else {
				stats.cutoff++;
			}
		}
		return selected;
	}
	
	protected final void selectionPrune(float[] val, int[] selection, int n, int s) {
		
		if(javaQueue){
			if(queue == null)
				queue = new PriorityQueue<FloatMatrixSlice.QueueItem>(select);
			
			for(int i = 0; i < s; i++) {
				int sel = selection[i];
				queue.offer(new QueueItem(sel, val[sel]));
			}
			
			for(int i = s; i < n; i++) {
				int sel = selection[i];
				float v = val[sel];
				
				if(v > queue.peek().val){
					queue.poll();
					queue.add(new QueueItem(sel, v));
				}
			}
		} else {
			if(queue == null)
				queue = new FibonacciHeap<FloatMatrixSlice.QueueItem>(select);
			
			
			for(int i = 0; i < n; i++) {
				int sel = selection[i];
				queue.offer(new QueueItem(sel, val[sel]));
			}
		}
		
		
		int selected = 0;
		
		for(QueueItem item : queue){
			selection[selected++] = item.idx;
		}
		
		queue.clear();
		Arrays.sort(selection, 0, s);
	}
	
	private static final class QueueItem implements Comparable<QueueItem>{
		public final int idx;
		public final float val;
		
		public QueueItem(int idx, float val){
			this.idx = idx; this.val = val;
		}
		
		@Override
		public int compareTo(QueueItem o) {
			return val > o.val ? 1 : -1; // unsafe. we assume non NaN floats
		}
	}
	
	protected final float computeTreshold(double avg, double max) {
		double tresh = pruneA*avg*(1.0f-pruneB*(max-avg));
		tresh = tresh < 1.0e-7f ? 1.0e-7f : tresh;
		return (float) (tresh < max ? tresh : max);
	}
	
	protected final void normalize(float[] val, int s, int t) {
		
		double sum = 0.0f;
		
		for(int i = t; i > s;){
			sum += val[--i];
		}		
		
		for(int i = t; i > s;){
			val[--i] /= sum;			
		}
	}
	
}
