/**
 * 
 */
package util;

import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.imageio.ImageIO;

import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceEntry;
import io.writables.SliceId;
import mapred.MCLConfigHelper;
import mapred.MCLContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class ReadClusters<M extends MCLMatrixSlice<M>> extends AbstractUtil {

	private static final Logger logger = LoggerFactory.getLogger(ReadClusters.class);
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ReadClusters(), args));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected int run(Path input, Path output, boolean hdfsOutput) throws Exception {
		
		Configuration conf = getConf();
		MatrixMeta meta = MatrixMeta.load(conf, input);
		
		if (meta == null) {
			return -1;
		}
		
		final int nsub = meta.getNSub();
		
		meta.apply(conf);
		FileSystem fs = input.getFileSystem(conf);
		
		PathFilter filter = new PathFilter() {
			
			@Override
			public boolean accept(Path path) {
				String name = path.getName();
				return !name.startsWith("_") && !name.startsWith(".");
			}
		};
		
		SliceId id = new SliceId();
		M m = null;
		Map<Long,Long> parents = new HashMap<Long, Long>(256*1024);
		Map<Long,List<Long>> attractors = new HashMap<Long, List<Long>>(256*1024);//TODO n
		
		for(FileStatus fileStatus : fs.listStatus(input,filter)) {
			Option pathOption = SequenceFile.Reader.file(fileStatus.getPath());
			SequenceFile.Reader reader = new SequenceFile.Reader(conf, pathOption);
			
			if(m == null) {
				MCLConfigHelper.setMatrixSliceClass(conf, (Class<? extends MCLMatrixSlice<?>>) reader.getValueClass());
				m = MCLContext.getMatrixSliceInstance(conf);
			}
			
			while (reader.next(id, m)) {
				for(SliceEntry e : m.dump()) {
					final Long j = ((long) id.getSliceId()*(long) nsub) + e.col;
					Long i = e.row;
					
//					if(e.val != 1.0f) {
//						System.out.printf("i: %d, j: %d, v: %f, diff: %e\n",i,j,e.val,e.val-0.5f);
//					}
					
					if(parents.containsKey(i)){
						i = parents.get(i);
						
						if(i.equals(j)){
							continue;
						}
					} else {
						if(i.equals(j)){
							if(!attractors.containsKey(i)) attractors.put(i, new LinkedList<Long>());
							continue;
						}
					}
					
					parents.put(j,i);
					List<Long> attracted = attractors.get(i);
					
					if(attracted == null){
						attracted = new LinkedList<Long>();
						attractors.put(i, attracted);
					}
					
					attracted.add(j);
					
					List<Long> existing = attractors.remove(j);
					
					if(existing != null){
						existing.remove(i);
						attracted.addAll(existing);
					}
					
				}
			}
			
			reader.close();
		}
				
		int min = Integer.MAX_VALUE;
		int max = 0;
		double avg = 0.0;
		
		File clusters = new File("clusters");
		BufferedWriter writer = new BufferedWriter(new FileWriter(clusters));
		for(Entry<Long, List<Long>> e : attractors.entrySet()){
			int cnt = 1 + e.getValue().size();
			min = Math.min(min, cnt);
			max = Math.max(max, cnt);
			avg += cnt;
			writer.write(e.getKey().toString());
			for(Long node : e.getValue()){
				writer.write('\t');
				writer.write(node.toString());
			}
			writer.newLine();
		}
		writer.close();
		
		logger.info("clusters: {}",attractors.size());
		logger.info("avg: {}",avg/attractors.size());
		logger.info("min: {}",min);
		logger.info("max: {}",max);
		
		return 0;
	}
}
