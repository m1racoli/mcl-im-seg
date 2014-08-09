package mapred;

import io.writables.SliceIndex;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public final class SlicePartitioner<V> extends Partitioner<SliceIndex, V> implements Configurable {

	private int numBlocks;
	
	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getPartition(SliceIndex key, V value, int numPartitions) {
		return (key.getSliceId() * numPartitions) / numBlocks;
	}
	
}