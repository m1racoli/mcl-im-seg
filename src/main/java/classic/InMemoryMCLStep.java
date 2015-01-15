package classic;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceId;
import io.writables.SubBlock;
import mapred.MCLConfigHelper;
import mapred.MCLContext;
import mapred.MCLResult;
import mapred.MCLStats;
import mapred.job.AbstractMCLJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryMCLStep extends AbstractMCLJob {
	
	private static final Logger logger = LoggerFactory.getLogger(InMemoryMCLStep.class);
	
	private final class StepRunner<M extends MCLMatrixSlice<M>> {
		
		private double sqd = 0.0;
		private int in_nnz = 0;
		private int out_nnz = 0;
		MCLStats stats = new MCLStats();
		
		TreeMap<Integer, M> map = new TreeMap<Integer, M>();
		
		private void run(Path left, Path right, Path prev, Path output) throws IOException {
			
			{				
				Reader lReader = new Reader(getConf(), Reader.file(left));
				SliceId lkey = new SliceId(-1);
				M lm = MCLContext.getMatrixSliceInstance(getConf());
				
				Reader rReader = new Reader(getConf(), Reader.file(right));
				SliceId rkey = new SliceId();
				SubBlock<M> subBlock = new SubBlock<M>();
				subBlock.setConf(getConf());
				
				Reader pReader = prev == null ? null : new Reader(getConf(), Reader.file(prev));
				SliceId pkey = prev == null ? null : new SliceId();
				M pm = prev == null ? null : MCLContext.<M>getMatrixSliceInstance(getConf());
				
				while(rReader.next(rkey,subBlock)){
					
					while(lkey.get() < rkey.get()){
						if(!lReader.next(lkey, lm)){
							rReader.close();
							lReader.close();
							if(prev != null) pReader.close();
							throw new RuntimeException("out of matrix slices. sub blocks left");
						}
						
						in_nnz += lm.size();
						
						if(prev != null){
							if(!pReader.next(pkey, pm)){
								rReader.close();
								lReader.close();
								if(prev != null) pReader.close();
								throw new RuntimeException("missing previous matrix slice");
							}
							sqd += lm.sumSquaredDifferences(pm);
						}
					}
					
					M m = subBlock.subBlock.multipliedBy(lm, null);
					add(subBlock.id,m);
				}
				
				while(lReader.next(lkey, lm)){
					in_nnz += lm.size();
					
					if(prev != null){
						if(!pReader.next(pkey, pm)){
							rReader.close();
							lReader.close();
							if(prev != null) pReader.close();
							throw new RuntimeException("missing previous matrix slice");
						}
						sqd += lm.sumSquaredDifferences(pm);
					}
				}
				
				rReader.close();
				lReader.close();
				if(prev != null) pReader.close();
			}
			
			Writer writer = SequenceFile.createWriter(getConf(),
					Writer.file(output),
					Writer.keyClass(SliceId.class),
					Writer.valueClass(MCLContext.getMatrixSliceClass(getConf())));
			
			Iterator<Entry<Integer,M>> it = map.entrySet().iterator();
			SliceId id = new SliceId();
			
			while(it.hasNext()){
				Entry<Integer,M> e = it.next();
				id.set(e.getKey());
				M m = e.getValue();
				
				m.inflateAndPrune(stats, null);
				out_nnz += m.size();
				writer.append(id, m);
				it.remove();
			}
			
			writer.close();			
		}
		
		private void add(int id, M m){
			M e = map.get(id);
			
			if(e == null){
				map.put(id, m.deepCopy());
				return;
			}
			e.add(m);
		}
		
	}
	
	@Override
	protected MCLResult run(List<Path> inputs, Path output) throws Exception {
		
		if(inputs == null || inputs.size() < 2 || output == null){
			throw new RuntimeException(String.format("invalid input/output: in=%s, out=%s",inputs,output));
		}
		//TODO calculate change
		final Configuration conf = getConf();
		
		MatrixMeta meta = MatrixMeta.load(conf, inputs.get(0));
		MatrixMeta meta1 = MatrixMeta.load(conf, inputs.get(1));
		
		if(inputs.size() == 2){
			logger.debug("num inputs = 2. mcl step without comparison of iterants");
			MatrixMeta.check(meta,meta1);
			meta.setKmax(meta.getKmax() * meta1.getKmax());
		} else {
			logger.debug("num inputs > 2. mcl step with comparison of iterants");
			MatrixMeta meta2 = MatrixMeta.load(conf, inputs.get(2));
			MatrixMeta.check(meta,meta1,meta2);
			meta.setKmax(Math.max(meta.getKmax() * meta1.getKmax(),meta2.getKmax()));
		}
		
		meta.apply(conf);
		
		Path left = inputs.get(0);
		Path right = inputs.get(1);
		Path prev = inputs.size() > 2 ? inputs.get(2) : null;
		
		@SuppressWarnings("rawtypes")
		StepRunner runner = new StepRunner();
		
		FileSystem fs = left.getFileSystem(getConf());
		RemoteIterator<LocatedFileStatus> it = fs.listFiles(left, false);
		
		while(it.hasNext()){
			LocatedFileStatus status = it.next();
			String filename = status.getPath().getName();
			if(filename.startsWith(".") || filename.startsWith("_"))
				continue;
			runner.run(status.getPath(), new Path(right,filename), prev == null ? null : new Path(prev, filename), new Path(output,filename));
		}

		MCLResult result = new MCLResult();
		result.success = true;
		result.counters = new Counters();
		result.in_nnz = runner.in_nnz;
		result.out_nnz = runner.out_nnz;
		result.cutoff = runner.stats.cutoff;
		result.prune = runner.stats.prune;
		
		meta.setKmax(runner.stats.kmax);
		MatrixMeta.save(conf, output, meta);
		
//		result.kmax = meta.getKmax();
//		result.in_nnz = job.getCounters().findCounter(Counters.MAP_INPUT_VALUES).getValue();
//		result.out_nnz = job.getCounters().findCounter(Counters.REDUCE_OUTPUT_VALUES).getValue();
//		result.attractors = job.getCounters().findCounter(Counters.ATTRACTORS).getValue();
//		result.homogenous_columns = job.getCounters().findCounter(Counters.HOMOGENEOUS_COLUMNS).getValue();
//		result.cutoff = job.getCounters().findCounter(Counters.CUTOFF).getValue();
//		result.prune = job.getCounters().findCounter(Counters.PRUNE).getValue();
		result.chaos = runner.stats.maxChaos;
		result.changeInNorm = prev != null ? Math.sqrt(runner.sqd)/meta.getN() : Double.POSITIVE_INFINITY;
		return result;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new InMemoryMCLStep(), args));
	}
}
