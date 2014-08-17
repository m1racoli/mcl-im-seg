/**
 * 
 */
package mapred;

/**
 * @author Cedrik Neumann
 *
 */
public class MCLResult {

	public boolean success = false;
	public int kmax = -1;
	public long n = -1;
	public long nnz = -1;
	public long attractors = -1;
	public long homogenous_columns = -1;
	public long cutoff = -1;
	public long prune = -1;
	
	@Override
	public String toString() {
		return String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",success,kmax,n,nnz,attractors,homogenous_columns,cutoff,prune);
	}
	
}
