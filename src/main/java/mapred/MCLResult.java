/**
 * 
 */
package mapred;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;

/**
 * result instance of an mcl job.
 * 
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
		StringWriter writer = new StringWriter();
		
		writer.append("Result[status: " + (success ? "SUCCESS": "FAILED"));
		
		if(kmax >= 0) writer.append(", kmax: " + kmax);
		if(n >= 0) writer.append(", n: "+n);
		if(in_nnz >= 0) writer.append(", in_nnz: "+in_nnz);
		if(out_nnz >= 0) writer.append(", out_nnz: "+out_nnz);
		if(attractors >= 0) writer.append(", attractors: "+attractors);
		if(homogenous_columns >= 0) writer.append(", homogenous_columns: "+homogenous_columns);
		if(cutoff >= 0) writer.append(", cutoff: "+cutoff);
		if(prune >= 0) writer.append(", prune: "+prune);
		if(runningtime >= 0) writer.append(", runningtime: "+runningtime);
		if(cpu_millis >= 0) writer.append(", cpu_millis: "+cpu_millis);
		
		return writer.toString();
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
