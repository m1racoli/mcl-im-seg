/**
 * 
 */
package mapred;

/**
 * container for mcl algorithm specific metrics in the inflation phase
 * 
 * @author Cedrik
 *
 */
public final class MCLStats {

	public double chaos = 0.0;
	public int kmax = 0;
	public long prune = 0;
	public long cutoff = 0;
	public long attractors = 0;
	public long homogen = 0;
	
	@Override
	public String toString() {
		return String.format("[maxChaos: %2.1f, kmax: %d, prune: %d, cutoff: %d, attractors: %d, homogen: %d]", chaos, kmax, prune, cutoff,attractors,homogen);
	}

	public void reset() {
		chaos = 0.0;
		kmax = 0;
		prune = 0;
		cutoff = 0;
		attractors = 0;
		homogen = 0;
	}
}
