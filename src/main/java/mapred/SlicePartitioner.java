package mapred;

import io.writables.SliceIndex;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public final class SlicePartitioner<V> extends Partitioner<SliceIndex, V> implements Configurable {

	private int numBlocks;
	
	@Override
	public void setConf(Configuration conf) {
		long n = MCLConfigHelper.getN(conf);
		int nsub = MCLConfigHelper.getNSub(conf);
		numBlocks = (int) Math.ceil((double) n/nsub);		
	}

	@Override
	public Configuration getConf() {
		return new Configuration();
	}

	@Override
	public int getPartition(SliceIndex key, V value, int numPartitions) {
		return (key.getSliceId() * numPartitions) / numBlocks;
	}
	
}