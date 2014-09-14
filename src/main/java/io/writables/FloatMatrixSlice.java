/**
 * 
 */
package io.writables;

import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import mapred.Counters;

/**
 * @author Cedrik
 *
 */
public abstract class FloatMatrixSlice<M extends FloatMatrixSlice<M>> extends MCLMatrixSlice<M> {

	private Queue<QueueItem> queue = null;
	
	protected final void inflate(float[] val, int s, int t) {
		final double I = inflation;
		for(int i = s; i < t; i++) {
			val[i] = (float) Math.pow(val[i], I);
		}
	}
	
	protected final int prune(float[] val, int s, int t, int[] selection, TaskAttemptContext context) {
		
		final int k = t-s;
		float sum = 0.0f;
		float max = 0.0f;
		final int S = select;
		
		for(int i = s; i < t; i++){
			final float v = val[i];
			sum += v;
			if(max < v)
				max = v;
		}
		
		final float tresh = cutoff * sum;//computeTreshold(sum/k, max);
		int selected = 0;		
		
		for(int i = s; i < t; i++){
			if(val[i] >= tresh){
				selection[selected++] = i;
			} else {
				if(context != null) context.getCounter(Counters.CUTOFF).increment(1);
			}
		}
		
		if(selected > S){
			if(context != null) context.getCounter(Counters.PRUNE).increment(selected - S);
			select(val, selection, selected, S);
			selected = S;
		}
		
		return selected;
	}
	
	private final void select(float[] val, int[] selection, int n, int s) {
		
		if(queue == null) {
			queue = new PriorityQueue<FloatMatrixSlice.QueueItem>(select);
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
		public final float val;
		
		public QueueItem(int idx, float val){
			this.idx = idx; this.val = val;
		}
		
		@Override
		public int compareTo(QueueItem o) {
			return val > o.val ? 1 : val < o.val ? -1 : 0; // unsafe. we assume non NaN floats
		}
	}
	
	private final float computeTreshold(float avg, float max) {
		//TODO a,b
		float tresh = 0.9f*avg*(1-2.0f*(max-avg));
		tresh = tresh < cutoff ? cutoff : tresh;
		return tresh < max ? tresh : max;
	}
	
	protected final void normalize(float[] val, int s, int t, TaskAttemptContext context) {
		
		switch (t - s) {
		case 0:
			if (context != null) {
				context.getCounter(Counters.EMPTY_COLUMNS).increment(1);
			}
			return;
		case 1:
			if (context != null) {
				context.getCounter(Counters.HOMOGENEOUS_COLUMNS).increment(1);
				context.getCounter(Counters.ATTRACTORS).increment(1);
			}
			return;
		default:
			break;
		}
		
		float min = Float.MAX_VALUE;
		float max = 0.0f;
		float sum = 0.0f;
		
		for(int i = s; i < t; i++){
			sum += val[i];
		}
		
		for(int i = s; i < t; i++){
			final float v = val[i] / sum;
			if(context != null && v > 0.5f){
				context.getCounter(Counters.ATTRACTORS).increment(1);
			}
			
			if(min > v) min = v;
			if(max < v) max = v;
			val[i] = v;
		}
		
		if(context != null && max-min <= 1e-6f){
			context.getCounter(Counters.HOMOGENEOUS_COLUMNS).increment(1);
		}
		
	}
	
}
