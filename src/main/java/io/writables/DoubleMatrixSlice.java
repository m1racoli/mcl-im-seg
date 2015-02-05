/**
 * 
 */
package io.writables;

import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Queue;

import mapred.MCLStats;

/**
 * @author Cedrik
 *
 */
public abstract class DoubleMatrixSlice<M extends DoubleMatrixSlice<M>> extends MCLMatrixSlice<M> {

	private Queue<QueueItem> queue = null;
	
	protected final void inflate(double[] val, int s, int t) {
		final double I = inflation;
		for(int i = s; i < t; i++) {
			val[i] = Math.pow(val[i], I);
		}
	}
	
	protected final int prune(double[] val, int s, int t, int[] selection, MCLStats stats) {
		
		final int k = t-s;
		double sum = 0.0f;
		double max = 0.0f;
		final int S = select;
		
		for(int i = s; i < t; i++){
			final double v = val[i];
			sum += v;
			if(max < v)
				max = v;
		}
		
		final double tresh = computeTreshold(sum/k, max);
		int selected = 0;		
		
		for(int i = s; i < t; i++){
			if(val[i] >= tresh){
				selection[selected++] = i;
			} else {
				stats.cutoff++;
			}
		}
		
		if(selected > S){
			stats.prune += selected-S;
			select(val, selection, selected, S);
			selected = S;
		}
		
		return selected;
	}
	
	private final void select(double[] val, int[] selection, int n, int s) {
		
		if(queue == null) {
			queue = new PriorityQueue<DoubleMatrixSlice.QueueItem>(select);
		}
		
		for(int i = 0; i < s; i++) {
			int sel = selection[i];
			queue.add( new QueueItem(sel, val[sel]));
		}
		
		for(int i = s; i < n; i++) {
			queue.remove();
			int sel = selection[i];
			queue.add( new QueueItem(sel, val[sel]));
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
		public final double val;
		
		public QueueItem(int idx, double val){
			this.idx = idx; this.val = val;
		}
		
		@Override
		public int compareTo(QueueItem o) {
			return val > o.val ? 1 : val < o.val ? -1 : 0; // unsafe. we assume non NaN floats
		}
	}
	
	private final double computeTreshold(double avg, double max) {
		//TODO a,b
		double tresh = 0.9f*avg*(1-2.0f*(max-avg));
		tresh = tresh < cutoff ? cutoff : tresh;
		return tresh < max ? tresh : max;
	}
	
	/**
	 * 
	 * @param val
	 * @param s
	 * @param t
	 * @param context
	 * @return chaos
	 */
	protected final float normalize(double[] val, int s, int t, MCLStats stats) {
		
		if(s == t){
			return 0.0f;
		}
		
		
		double sum = 0.0f;
		
		for(int i = s; i < t; i++){
			sum += val[i];
		}
		
		if(sum == 0.0){
			return 0.0f;
		}
		
		double min = Float.MAX_VALUE;
		double max = 0.0f;
		double sumsq = 0.0;
		
		for(int i = s; i < t; i++){
			final double v = val[i] / sum;
			if(stats != null && v > 0.5f){
				stats.attractors++;
			}
			
			sumsq += v*v;

			if(min > v) {
				min = v;
			} else {
				if(max < v) max = v;
			}

			val[i] = v;
		}
		
		if(stats != null && min == max){
			stats.homogen++;
		}
		
		return (float) (max - sumsq) * (t-s);
	}
	
}
