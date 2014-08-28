package io.writables;

import io.writables.MCLMatrixSlice.MatrixEntry;

import java.awt.Image;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

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

import util.MatrixSpy;

public class SubDimTest {
	
	private static final Logger logger = LoggerFactory.getLogger(SubDimTest.class);
	private static final int n = 100;
	private static final int k = 100;
	private static final File abcFile = new File("examples\\10x10.abc");
	
	
	public static <M extends MCLMatrixSlice<M>>void main(String[] args) throws Exception {
		
		int[] nsubs = new int[]{1,5,10,25,50,100};
		
		Map<Integer,Map<SliceId,M>> matrices = new HashMap<Integer,Map<SliceId,M>>();
		
		OpenMapRealMatrix oo = matrixFromAbc(n, abcFile);
		OpenMapRealMatrix o2 = oo.multiply(oo);
		ImageIO.write(MatrixSpy.getGrayScale(oo.getData()), "png", new File("subdimspy_oo.png"));
		ImageIO.write(MatrixSpy.getGrayScale(o2.getData()), "png", new File("subdimspy_o2.png"));
		
		for(int nsub : nsubs) {
			Configuration conf = getConf();
			MCLConfigHelper.setNSub(conf, nsub);
			Map<SliceId, M> m = fromAbc(conf, abcFile);
			System.out.printf("%d\t%e\n",nsub,oo.subtract(toMatrix(m, nsub, n)).getFrobeniusNorm());
			
			
			ImageIO.write(MatrixSpy.getGrayScale(toMatrix(m, nsub, n).getData()), "png", new File("matrix_pre_"+nsub+".png"));
			ImageIO.write(MatrixSpy.getGrayScale(oo.subtract(toMatrix(m, nsub, n)).getData()), "png", new File("diff_pre_"+nsub+".png"));
			m = iterate(conf, m,false);
			matrices.put(nsub, m);
			OpenMapRealMatrix o = toMatrix(m, nsub, n);
			OpenMapRealMatrix diff = o2.subtract(o);
			System.out.printf("%d\t%e\n",nsub,diff.getFrobeniusNorm());
			ImageIO.write(MatrixSpy.getGrayScale(o.getData()), "png", new File("matrix_"+nsub+".png"));
			ImageIO.write(MatrixSpy.getGrayScale(diff.getData()), "png", new File("diff_"+nsub+".png"));
		}
		
		for(int nsub1 : nsubs){
			for(int nsub2 : nsubs){
				ImageIO.write(MatrixSpy.getGrayScale(toMatrix(matrices.get(nsub1), nsub1, n).subtract(toMatrix(matrices.get(nsub2), nsub2, n)).getData()), "png", new File("crossdiff_"+nsub1+"_"+nsub2+".png"));
			}
		}
		
	}
	
	public static final Configuration getConf() {
		Configuration conf = new Configuration();
		MCLConfigHelper.setN(conf, n);
		MCLConfigHelper.setKMax(conf, k);
		MCLConfigHelper.setPrintMatrix(conf, PrintMatrix.ALL);
		MCLConfigHelper.setCutoffInv(conf, 100000);
		MCLConfigHelper.setDebug(conf, true);
		MCLConfigHelper.setSelection(conf, 2);
		MCLConfigHelper.setUseVarints(conf, true);
		MCLConfigHelper.setMatrixSliceClass(conf, CSCDoubleSlice.class);
		MCLContext.setLogging(conf);
		return conf;
	}
	
	public static final <M extends MCLMatrixSlice<M>> Map<SliceId, M> iterate(Configuration conf, Map<SliceId, M> m, boolean inf_prune) throws IOException {
		
		Map<SliceId,List<SubBlock<M>>> subBlocks = new LinkedHashMap<SliceId, List<SubBlock<M>>>();
		
		int cnt = 0;
		for (Entry<SliceId,M> e : m.entrySet()) {
			SliceId outId = new SliceId();
			for (M s : e.getValue().getSubBlocks(outId)) {
				cnt++;
				SliceId id = new SliceId();
				id.set(outId.get());
				if(!subBlocks.containsKey(id)) {
					subBlocks.put(id, new ArrayList<SubBlock<M>>());
				}
				SubBlock<M> subBlock = new SubBlock<M>();
				subBlock.setConf(conf, false);
				subBlock.id = e.getKey().get();
				subBlock.subBlock = rewrite(s, conf);
				subBlocks.get(id).add(subBlock);
			}
		}
		
		logger.debug("nsub: {}, num subblocks: {}",MCLConfigHelper.getNSub(conf),cnt);
		
		Map<SliceId,M> newSlices = new LinkedHashMap<SliceId, M>();
		
		for (SliceId id : subBlocks.keySet()) {			
			for(SubBlock<M> subBlock : subBlocks.get(id)) {
				SliceId blockId = new SliceId();
				blockId.set(subBlock.id);
				if(!newSlices.containsKey(blockId)){
					newSlices.put(blockId, MCLContext.<M>getMatrixSliceInstance(conf));
				}
				newSlices.get(blockId).add(rewrite(subBlock.subBlock.multipliedBy(m.get(id), null),conf));				
			}
		}
		
		if(inf_prune){
			for(M slice : newSlices.values()){
				slice.inflateAndPrune(null);
			}
		}
		
		return newSlices;
	}
	
	public static final OpenMapRealMatrix matrixFromAbc(int n, File file) throws IOException {
		Pattern pattern = Pattern.compile("\t");
		OpenMapRealMatrix o = new OpenMapRealMatrix(n, n);
		BufferedReader reader = null;
		
		try {
			reader = new BufferedReader(new FileReader(file));
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				String[] split = pattern.split(line);
				if(split.length != 3){
					throw new RuntimeException("bad split length: "+split.length);
				}
				
				int col = Integer.parseInt(split[0]);
				int row = Integer.parseInt(split[1]);
				float val = Float.parseFloat(split[2]);
				
				o.setEntry(row, col, val);
			}
		} finally {
			if(reader != null) reader.close();
		}
		
		for(int col = 0; col < n; col++){
			float sum = 0.0f;
			
			double[] column = o.getColumn(col);
			for(int row = 0; row < n; row++){
				sum += column[row];
			}
			
			for(int row = 0; row < n; row++){
				column[row] /= sum;
			}
			
			o.setColumn(col, column);
		}
		
		return o;
	}
	
	public static final <M extends MCLMatrixSlice<M>> Map<SliceId, M> fromAbc(Configuration conf, File file) throws IOException {
		
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
		
		Map<SliceId, M> slices = new LinkedHashMap<SliceId, M>();
		
		for(SliceId id : entries.keySet()) {
			M slice = MCLContext.<M>getMatrixSliceInstance(conf);
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
	
	public static <M extends MCLMatrixSlice<M>> OpenMapRealMatrix toMatrix(Map<SliceId, M> m, int nsub, int n){
		OpenMapRealMatrix o = new OpenMapRealMatrix(n, n);
		
		for(SliceId id : m.keySet()){
			M slice = m.get(id);			
			for(MatrixEntry e : slice.dump()) {
				o.setEntry((int) e.row, e.col+ (id.get()*nsub), e.val);
			}
		}
		
		return o;
	}
	
	public static <M extends MCLMatrixSlice<M>> M rewrite(M s, Configuration conf) throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		s.write(new DataOutputStream(outputStream));
		M o = MCLContext.<M>getMatrixSliceInstance(conf);
		ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
		o.readFields(new DataInputStream(inputStream));
		return o;
	}
}
