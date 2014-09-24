/**
 * 
 */
package mapred;

/**
 * @author Cedrik
 *
 */
public final class MCLStats {

	public double maxChaos = 0.0;
	public int kmax = 0;
	
	@Override
	public String toString() {
		return String.format("[maxChaos: %2.1f, kmax: %d]", maxChaos, kmax);
	}
}
