/**
 * 
 */
package mapred;

/**
 * @author Cedrik
 *
 */
public final class MCLOut {

	private static final String HEADER = " ite --------------------  chaos  time expa expb expc";
	
	private MCLOut(){};
	
	public static void init() {
		System.out.println(HEADER);
	}
	
	public static void startIteration(int i) {
		System.out.printf("%3d  ",i);
	}
	
	public static void progress(float last, float current) {
		if(last >= current)
			return;
		
		int diff = ((int) (100.0f * current)/5) - ((int) (100.0f * last)/5);
		//print diff times a '.'
		System.out.print(new String(new char[diff]).replace('\0', '.'));
	}
	
	/**
	 * 
	 * @param vals
	 */
	public static void stats(Object ... vals) {
		System.out.printf("%7.2f %5.2f %4.2f %4.2f %4.2f", vals);
	}
	
	public static void finishIteration() {
		System.out.println();
	}
}
