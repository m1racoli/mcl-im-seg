package io.writables;

import io.writables.MCLMatrixSlice.MatrixEntry;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

import mapred.MCLConfigHelper;
import mapred.MCLContext;
import mapred.PrintMatrix;

import org.apache.commons.math3.linear.DefaultRealMatrixPreservingVisitor;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealMatrixPreservingVisitor;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubDimTest {
	
	private static final Logger logger = LoggerFactory.getLogger(SubDimTest.class);
	private static final long n = 100;
	private static final int k = 100;
	private static final File abcFile = new File("examples\\10x10.abc");
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf1 = getConf();
		MCLConfigHelper.setNSub(conf1, 5);
		Configuration conf2 = getConf();
		MCLConfigHelper.setNSub(conf2, 10);
		Map<SliceId, CSCSlice> m1 = fromAbc(conf1, abcFile);
		Map<SliceId, CSCSlice> m2 = fromAbc(conf2, abcFile);
		
		m1 = iterate(conf1, m1);
		m2 = iterate(conf2, m2);
		
		
	}
	
	public static final Configuration getConf() {
		Configuration conf = new Configuration();
		MCLConfigHelper.setN(conf, n);
		MCLConfigHelper.setKMax(conf, k);
		MCLConfigHelper.setPrintMatrix(conf, PrintMatrix.ALL);
		MCLConfigHelper.setDebug(conf, true);
		MCLConfigHelper.setSelection(conf, 2);
		MCLConfigHelper.setUseVarints(conf, true);
		MCLContext.setLogging(conf);
		return conf;
	}
	
	public static final Map<SliceId, CSCSlice> iterate(Configuration conf, Map<SliceId, CSCSlice> slices) throws IOException {
		
		Map<SliceId,List<SubBlock<CSCSlice>>> subBlocks = new LinkedHashMap<SliceId, List<SubBlock<CSCSlice>>>();
		
		for (Entry<SliceId,CSCSlice> e : slices.entrySet()) {
			SliceId outId = new SliceId();
			for (CSCSlice s : e.getValue().getSubBlocks(outId)) {
				SliceId id = new SliceId();
				id.set(outId.get());
				if(!subBlocks.containsKey(id)) {
					subBlocks.put(id, new ArrayList<SubBlock<CSCSlice>>());
				}
				SubBlock<CSCSlice> subBlock = new SubBlock<CSCSlice>();
				subBlock.setConf(conf, false);
				subBlock.id = e.getKey().get();
				subBlock.subBlock = rewrite(s, conf);
				subBlocks.get(id).add(subBlock);
			}
		}
		
		Map<SliceId,CSCSlice> newSlices = new LinkedHashMap<SliceId, CSCSlice>();
		
		for (SliceId id : subBlocks.keySet()) {			
			for(SubBlock<CSCSlice> subBlock : subBlocks.get(id)) {
				SliceId blockId = new SliceId();
				blockId.set(subBlock.id);
				if(!newSlices.containsKey(blockId)){
					newSlices.put(blockId, MCLContext.<CSCSlice>getMatrixSliceInstance(conf));
				}
				newSlices.get(blockId).add(rewrite(subBlock.subBlock.multipliedBy(slices.get(id), null),conf));				
			}
		}
		
		for(CSCSlice slice : newSlices.values()){
			slice.inflateAndPrune(null);
		}
		
		return newSlices;
	}
	
	public static final Map<SliceId, CSCSlice> fromAbc(Configuration conf, File file) throws IOException {
		
		int nsub = MCLConfigHelper.getNSub(conf);
		Pattern pattern = Pattern.compile("\t");
		Map<SliceId,SortedSet<MatrixEntry>> entries = new LinkedHashMap<SliceId, SortedSet<MatrixEntry>>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				String[] split = pattern.split(line);
				if(split.length != 3){
					throw new RuntimeException("bad split length: "+split.length);
				}
				
				long col = Long.parseLong(split[0]);
				long row = Long.parseLong(split[1]);
				float val = Float.parseFloat(split[2]);
				
				SliceId id = new SliceId();
				MatrixEntry e = new MatrixEntry();
				
				id.set(MCLContext.getIdFromIndex(col, nsub));
				e.col = MCLContext.getSubIndexFromIndex(col, nsub);
				e.row = row;
				e.val = val;
				
				if(!entries.containsKey(id)){
					entries.put(id, new TreeSet<MatrixEntry>());
				}
				entries.get(id).add(e);				
			}
			
		} finally {
			if(reader != null) reader.close();
		}
		
		if(logger.isDebugEnabled()){
			logger.debug("n: {}, nsub: {}, k: {}",MCLConfigHelper.getN(conf),nsub,MCLConfigHelper.getKMax(conf));
			for(SliceId id : entries.keySet()){
				logger.debug("slice: {}, nzz: {}",id,entries.get(id).size());
			}
		}
		
		Map<SliceId, CSCSlice> slices = new LinkedHashMap<SliceId, CSCSlice>();
		
		for(SliceId id : entries.keySet()) {
			CSCSlice slice = MCLContext.<CSCSlice>getMatrixSliceInstance(conf);
			slice.fill(entries.get(id));
			slices.put(id, slice);
		}
		
		return slices;
	}
	
	public static final <M extends MCLMatrixSlice<M>> M asMatrixSlice(RealMatrix m, Configuration conf) {
		M slice = MCLContext.<M>getMatrixSliceInstance(conf);
		int nsub = MCLConfigHelper.getNSub(conf);
		int k = MCLConfigHelper.getKMax(conf);
		int nnz = k * nsub;
		final int[] cols = new int[nnz];
		final long[] rows = new long[nnz];
		final float[] vals = new float[nnz];
		
		RealMatrixPreservingVisitor visitor = new DefaultRealMatrixPreservingVisitor() {
			int i = 0;
			@Override
			public void visit(int row, int column, double value) {
				cols[i] = column; rows[i] = row; vals[i++] = (float) value;
			}
		};
		m.walkInColumnOrder(visitor);
		slice.fill(cols, rows, vals);
		return slice;
	}
	
	public static final OpenMapRealMatrix getTestMatrix(int n, int nsub, int k) {
		OpenMapRealMatrix m = new OpenMapRealMatrix(n, nsub);		
		Random rand = new Random();		
		for (int j = 0; j < nsub; j++) {
			float sum = 0;
			for (int i = 0; i < k; i++) {
				int p = rand.nextInt(n);
				while(m.getEntry(p, j) != 0.0) {
					p = (p + 1) % n;
				}
				float val = (float) rand.nextDouble();
				sum += val;
				m.setEntry(p, j, val);
			}
			
			for(int i = 0; i < n; i++) {
				if(m.getEntry(i, j) != 0.0) {
					m.setEntry(i, j, m.getEntry(i, j)/sum);
				}
			}
		}
		return m;
	}
	
	public static final <M extends MCLMatrixSlice<M>> double dist(M m1, M m2) {
		OpenMapRealMatrix o = new OpenMapRealMatrix((int)n, (int)n);
		
		for(MatrixEntry e : m1.dump()) {
			o.setEntry((int) e.row, e.col, e.val);
		}
		
		for(MatrixEntry e : m2.dump()) {
			
		}
		//TODO
		return o.subtract(o2).getNorm();
	}
	
	public static CSCSlice rewrite(CSCSlice m, Configuration conf) throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		m.write(new DataOutputStream(outputStream));
		CSCSlice o = new CSCSlice(conf);
		ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
		o.readFields(new DataInputStream(inputStream));
		return o;
	}
}
