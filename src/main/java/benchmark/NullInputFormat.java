package benchmark;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class NullInputFormat extends InputFormat<NullWritable, NullWritable>{

	@Override
	public RecordReader<NullWritable, NullWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new NullRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		return Collections.<InputSplit>singletonList(new InputSplit() {
			
			@Override
			public String[] getLocations() throws IOException, InterruptedException {
				return new String[]{"localhost"};
			}
			
			@Override
			public long getLength() throws IOException, InterruptedException {
				return 0;
			}
		});
	}
	
	public static final class NullRecordReader extends RecordReader<NullWritable, NullWritable> {

		private boolean has_next = true;
		
		@Override
		public void close() throws IOException {}

		@Override
		public NullWritable getCurrentKey() throws IOException,
				InterruptedException {
			if(!has_next)
				return null;
			has_next = false;
			return NullWritable.get();
		}

		@Override
		public NullWritable getCurrentValue() throws IOException,
				InterruptedException {
			return NullWritable.get();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return has_next ? 0.0f : 1.0f;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return has_next;
		}
		
	}

}
