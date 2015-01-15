/**
 * 
 */
package mapred;

import java.io.OutputStream;
import java.io.PrintStream;

import org.apache.commons.io.output.TeeOutputStream;
import org.apache.hadoop.fs.Path;

/**
 * @author Cedrik
 *
 */
public final class MCLOut {

	private static final String HEADER = " ite --------------------  chaos   time expa expb expc   kmax change";
	
	private static PrintStream out;
	
	private MCLOut(){};
	
	public static void init() {
		init(null);
	}
	
	public static void init(OutputStream o) {
		if(out != null){
			throw new IllegalStateException("output already initialized");
		}
		
		if(o == null){
			out = System.out;
		} else {
			out = new PrintStream(new TeeOutputStream(System.out, o));
		}
	}
	
	public static void start(long n, int nsub, int te, int kmax){
		out.printf("n: %d, nsub: %d, paralellism: %d, kmax: %d\n",n,nsub,te,kmax);
		out.println(HEADER);
	}
	
	public static void startIteration(int i) {
		out.printf("%3d  ",i);
	}
	
	public static void progress(float last, float current) {
		if(last >= current)
			return;
		
		int diff = ((int) (100.0f * current)/5) - ((int) (100.0f * last)/5);
		//print diff times a '.'
		out.print(new String(new char[diff]).replace('\0', '.'));
	}
	
	/**
	 * 
	 * @param vals
	 */
	public static void stats(Object ... vals) {
		out.printf("%7.2f %6.2f %4.2f %4.2f %4.2f %6d", vals);
	}
	
	public static void change(double change) {
		out.printf(" %f",change);
	}
	
	public static void transpose(){
		out.printf("  transpose");
	}
	
	public static void finishIteration() {
		out.println();
	}
	
	public static void runningTime(long millis){
		out.printf("total runtime: %f seconds\n",(double) millis/1000.0);
	}
	
	public static void clusters(long num){
		out.printf("clusters found: %d\n",num);
	}
	
	public static void result(Path path){
		out.printf("Output written to: %s\n",path);
	}
	
	public static void println(String str){
		out.println(str);
	}
}
