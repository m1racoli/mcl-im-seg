/**
 * 
 */
package mapred;

import org.apache.hadoop.mapreduce.Job;

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
	public long runningtime = -1;
	
	public void run(Job job) throws Exception {
		success = job.waitForCompletion(true);
		runningtime = job.getFinishTime() - job.getStartTime();
	}
	
	@Override
	public String toString() {
		return String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d",success,kmax,n,nnz,attractors,homogenous_columns,cutoff,prune,runningtime);
	}
	
}
