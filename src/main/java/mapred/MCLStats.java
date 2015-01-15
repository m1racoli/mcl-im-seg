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
	public int prune = 0;
	public int cutoff = 0;
	
	@Override
	public String toString() {
		return String.format("[maxChaos: %2.1f, kmax: %d, prune: %d, cutoff: %d]", maxChaos, kmax, prune, cutoff);
	}

	public void reset() {
		maxChaos = 0.0;
		kmax = 0;
		prune = 0;
		cutoff = 0;
	}
}
