/**
 * 
 */
package mapred;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;

/**
 * @author Cedrik Neumann
 *
 */
public class MCLResult {

	public boolean success = false;
	public int kmax = -1;
	public long n = -1;
	public long in_nnz = -1;
	public long out_nnz = -1;
	public long attractors = -1;
	public long homogenous_columns = -1;
	public long cutoff = -1;
	public long prune = -1;
	public long runningtime = -1;
	public long cpu_millis = -1;
	public org.apache.hadoop.mapreduce.Counters counters = null;
	public double chaos = 1.0f;
	public double changeInNorm = Double.NaN;
	public long clusters = -1;
	
	public void run(Job job) throws Exception {
		success = job.waitForCompletion(true);
		runningtime = job.getFinishTime() - job.getStartTime();
		cpu_millis = job.getCounters().findCounter(TaskCounter.CPU_MILLISECONDS).getValue();
		counters = job.getCounters();
	}
	
	@Override
	public String toString() {
		return String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d",success,kmax,n,in_nnz,out_nnz,attractors,homogenous_columns,cutoff,prune,runningtime,cpu_millis);
	}
	
	public static void prepareCounters(File file) throws IOException {
		//file.createNewFile();
		FileWriter fileWriter = new FileWriter(file);
		BufferedWriter writer = new BufferedWriter(fileWriter);
		writer.write("iteration\tjob\tgroup\tcounter\tvalue");
		writer.newLine();
		writer.close();
		fileWriter.close();
	}
	
}
