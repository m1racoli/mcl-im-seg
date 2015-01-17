package mapred;

import java.util.Arrays;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.IStringConverter;

/**
 * 
 * basic selector implementation
 * 
 * @author Cedrik Neumann
 *
 */
public class Selector extends MCLInstance {
	
	private PriorityQueue<QueueItem> queue = null;
	
	public Selector(){};
	
	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		queue = new PriorityQueue<Selector.QueueItem>(MCLConfigHelper.getSelection(getConf()));
	}
	
	/**
	 * @param val
	 * @param selection
	 * @param n length of selection
	 * @param s target length of selection
	 * @return sum of new selection
	 * 
	 * @throws IllegalArgumentException if n <= s
	 */
	public final float select(float[] val, int[] selection, int n, int s) {
		
		if(n <= s){
			throw new IllegalArgumentException(String.format("n (%d) <= s (%d) is not allowed",n,s));
		}
		
		return implementSelect(val, selection, n, s);
	}
	
	protected float implementSelect(float[] val, int[] selection, int n, int s) {
		
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
		float sum = 0.0f;
		for(QueueItem item : queue){
			selection[selected++] = item.idx;
			sum += item.val;
		}
		queue.clear();
		Arrays.sort(selection, 0, s);
		
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
			return val > o.val ? 1 : val < o.val ? -1 : 0; // unsafe. we assume non NaN floats
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