package mapred;

import java.util.Arrays;
import java.util.PriorityQueue;

import com.beust.jcommander.IStringConverter;

/**
 * 
 * basic selector implementation
 * 
 * @author Cedrik Neumann
 *
 */
public class Selector extends MCLContext {
	
	private final PriorityQueue<QueueItem> queue = new PriorityQueue<Selector.QueueItem>(getSelection());
	 
	/**
	 * @param val
	 * @param selection
	 * @param n
	 * @param k
	 * @return sum of new selection
	 * 
	 * @throws IllegalArgumentException if k <= n
	 */
	public final float select(float[] val, int[] selection, int n, int k) {
		
		if(k <= n){
			throw new IllegalArgumentException(String.format("k (%d) <= n (%d) is not allowed",k,n));
		}
		
		return implementSelect(val, selection, n, k);
	}
	
	protected float implementSelect(float[] val, int[] selection, int n, int k) {
		
		for(int i = 0; i < k; i++) {
			if(queue.size() == n){
				queue.remove();
			}
			
			queue.add( new QueueItem(selection[i], val[i]));
		}
		
		int selected = 0;
		float sum = 0.0f;
		for(QueueItem item : queue){
			selection[selected++] = item.idx;
			sum += item.val;
		}
		
		Arrays.sort(selection, 0, n);
		
		return sum;
	}
	
	private static final class QueueItem implements Comparable<QueueItem>{
		public final int idx;
		public final float val;
		
		public QueueItem(int idx, float val){
			this.idx = idx; this.val = val;
		}
		
		@Override
		public int compareTo(QueueItem o) {
			return val > o.val ? -1 : val < o.val ? 1 : 0; // unsafe. we assume non NaN floats
		}
	}
	
	public static final class ClassConverter implements IStringConverter<Class<? extends Selector>> {

		@SuppressWarnings("unchecked")
		@Override
		public Class<? extends Selector> convert(String str) {
			try {
				return (Class<? extends Selector>) Class.forName(str);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
}
