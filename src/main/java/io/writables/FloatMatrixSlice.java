/**
 * 
 */
package io.writables;

import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import mapred.Counters;
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
	
	protected final int prune(float[] val, int s, int t, int[] selection, MCLStats stats, boolean auto) {
		
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
		
		final float tresh = auto ? computeTreshold(sum/k, max) : cutoff;
		int selected = 0;
		
		for(int i = s; i < t; i++){
			if(val[i] >= tresh){
				selection[selected++] = i;
			} else {
				stats.cutoff++;
				//if(context != null) context.getCounter(Counters.CUTOFF).increment(1);
			}
		}
		
		if(selected > S){
			stats.prune += selected - S;
			//if(context != null) context.getCounter(Counters.PRUNE).increment(selected - S);
			select(val, selection, selected, S);
			selected = S;
		}
		
		return selected;
	}
	
	protected final void select(float[] val, int[] selection, int n, int s) {
		
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
	
	protected final float computeTreshold(float avg, float max) {
		float tresh = pruneA*avg*(1.0f-pruneB*(max-avg));
		tresh = tresh < 1.0e-7f ? 1.0e-7f : tresh;
		return tresh < max ? tresh : max;
	}
	
	protected final void normalize(float[] val, int s, int t, TaskAttemptContext context) {
		
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
			//TODO interesting or do we use chaos?
			context.getCounter(Counters.HOMOGENEOUS_COLUMNS).increment(1);
		}
	}
	
}
