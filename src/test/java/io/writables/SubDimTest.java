package io.writables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
	
	@SuppressWarnings("rawtypes")
	public static <M extends MCLMatrixSlice<M>>void main(String[] args) throws Exception {
		
		int iterations = 10;
		int[] nsubs = new int[]{1,5,10,25,50,100};
		Class[] classes = new Class[]{CSCSlice.class,CSCDoubleSlice.class,OpenMapSlice.class};
		RealMatrix original = getRandom(n, 24235256L, 0.2);
		//RealMatrix original = matrixFromAbc(n,abcFile);		
		for(int nsub : nsubs) {
			runTests(nsub, classes, original, iterations);
		}
		
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <M extends MCLMatrixSlice<M>> void runTests(int nsub, Class[] classes, RealMatrix original, int iterations) throws IOException {
		
		final int size = classes.length;
		Configuration[] conf = new Configuration[size];
		List<Map<SliceId,M>> ms = new ArrayList<Map<SliceId,M>>(size);
		
		for(int i = 0; i < size; i++){
			conf[i] = getConf();
			MCLConfigHelper.setNSub(conf[i], nsub);
			MCLConfigHelper.setMatrixSliceClass(conf[i], classes[i]);
			Map<SliceId, M> m = asMatrixSliceSet(original, conf[i]);
			ms.add(m);
		}
		normalize(original);
		
		System.out.printf("nsub=%4d original\t", nsub);
		for(int i = 0; i < size; i++){
			System.out.printf("| %s\t\t%s\t\t%s\t\t", "norm","max","sum");
		}
		System.out.println();
		
		System.out.printf("%d\t%e\t",0,absMax(original));
		for(int i = 0; i < size; i++){
			RealMatrix diff = original.subtract(toMatrix(ms.get(i), nsub, n));
			System.out.printf("| %e\t%e\t%e\t",diff.getNorm(),absMax(diff),sum(diff));
		}
		System.out.println();
		
		for(int iter = 1; iter <= iterations; iter++){
			
			original = original.multiply(original);
			normalize(original);
			
			System.out.printf("%d\t%e\t",iter,absMax(original));
			if(iter == 1){
				ImageIO.write(MatrixSpy.getGrayScale(original.getData()), "png", new File("dump/matrix_ori_"+nsub+".png"));
			}
			for(int i = 0; i < size; i++){
				Map<SliceId,M> m = ms.get(i);
				m = iterate(conf[i], m);
				ms.set(i, m);
				RealMatrix diff = original.subtract(toMatrix(m, nsub, n));
				System.out.printf("| %e\t%e\t%e\t",diff.getNorm(),absMax(diff),sum(diff));
				if(iter == 1){
					ImageIO.write(MatrixSpy.getGrayScale(toMatrix(m, nsub, n).getData()), "png", new File("dump/matrix_"+i+"_"+nsub+".png"));
				}
				
			}
			System.out.println();
		}
		
		//ImageIO.write(MatrixSpy.getGrayScale(toMatrix(m, nsub, n).getData()), "png", new File("matrix_pre_"+nsub+".png"));
		//ImageIO.write(MatrixSpy.getGrayScale(oo.subtract(toMatrix(m, nsub, n)).getData()), "png", new File("diff_pre_"+nsub+".png"));
		
		//ImageIO.write(MatrixSpy.getGrayScale(o.getData()), "png", new File("matrix_"+nsub+".png"));
		//ImageIO.write(MatrixSpy.getGrayScale(diff.getData()), "png", new File("diff_"+nsub+".png"));
		
	}

	public static final Configuration getConf() {
		Configuration conf = new Configuration();
		MCLConfigHelper.setN(conf, n);
		MCLConfigHelper.setKMax(conf, k);
		MCLConfigHelper.setPrintMatrix(conf, PrintMatrix.ALL);
		//MCLConfigHelper.setCutoffInv(conf, 10000);
		MCLConfigHelper.setDebug(conf, true);
		//MCLConfigHelper.setSelection(conf, 2);
		//MCLConfigHelper.setUseVarints(conf, true);
		//MCLConfigHelper.setMatrixSliceClass(conf, CSCDoubleSlice.class);
		MCLContext.setLogging(conf);
		return conf;
	}
	
	public static final <M extends MCLMatrixSlice<M>> Map<SliceId, M> iterate(Configuration conf, Map<SliceId, M> m) throws IOException {
		
		Map<SliceId,List<SubBlock<M>>> subBlocks = new LinkedHashMap<SliceId, List<SubBlock<M>>>();
		
		for (Entry<SliceId,M> e : m.entrySet()) {
			SliceId outId = new SliceId();
			M slice = e.getValue().deepCopy();
			for (M s : slice.getSubBlocks(outId)) {
				SliceId id = new SliceId();
				id.set(outId.get());
				if(!subBlocks.containsKey(id)) {
					subBlocks.put(id, new ArrayList<SubBlock<M>>());
				}
				SubBlock<M> subBlock = new SubBlock<M>();
				subBlock.setConf(conf, false);
				subBlock.id = e.getKey().get();
				subBlock.subBlock = s.deepCopy();
				subBlocks.get(id).add(subBlock);
			}
		}
		
		//logger.debug("nsub: {}, num subblocks: {}",MCLConfigHelper.getNSub(conf),cnt);
		
		Map<SliceId,M> newSlices = new LinkedHashMap<SliceId, M>();
		
		for (SliceId id : m.keySet()) {
			M leftSlice = m.get(id);
			
			if(!subBlocks.containsKey(id)){
				continue;
			}
			
			for(SubBlock<M> subBlock : subBlocks.get(id)) {
				SliceId blockId = new SliceId();
				blockId.set(subBlock.id);
				if(!newSlices.containsKey(blockId)){
					newSlices.put(blockId, MCLContext.<M>getMatrixSliceInstance(conf));
				}
				newSlices.get(blockId).add(subBlock.subBlock.deepCopy().multipliedBy(leftSlice.deepCopy(), null));
			}
		}
		
		for(SliceId id : m.keySet()){
			if(!newSlices.containsKey(id)){
				newSlices.put(id, MCLMatrixSlice.<M>getInstance(conf));
			} else {
				newSlices.get(id).makeStochastic(null);
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
		
		normalize(o);
		
		return o;
	}
	
	public static final RealMatrix normalize(RealMatrix original) {
		for(int col = 0; col < n; col++){
			double sum = 0.0f;
			
			double[] column = original.getColumn(col);
			for(int row = 0; row < n; row++){
				sum += column[row];
			}
			
			if(sum == 0.0){
				Arrays.fill(column, 0.0);
			} else {
				for(int row = 0; row < n; row++){
					column[row] /= sum;
				}
			}

			original.setColumn(col, column);
		}
		return original;
	}
	
	public static final <M extends MCLMatrixSlice<M>> Map<SliceId, M> fromAbc(Configuration conf, File file) throws IOException {
		
		int nsub = MCLConfigHelper.getNSub(conf);
		Pattern pattern = Pattern.compile("\t");
		Map<SliceId,SortedSet<SliceEntry>> entries = new LinkedHashMap<SliceId, SortedSet<SliceEntry>>();
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
				id.set(MCLContext.getIdFromIndex(col, nsub));
				
				SliceEntry e = SliceEntry.get(MCLContext.getSubIndexFromIndex(col, nsub),row,val);
				
				if(!entries.containsKey(id)){
					entries.put(id, new TreeSet<SliceEntry>());
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
	
	public static final RealMatrix getRandom(int n, long seed, double dense){
		Random random = new Random(seed);
		OpenMapRealMatrix m = new OpenMapRealMatrix(n, n);

		for(int row = 0; row < n; row++){
			for(int col = 0; col < n; col++){
				if(random.nextDouble() < dense){
					m.setEntry(row, col, random.nextFloat());
				}
			}
		}
		
		return m;
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
	
	public static final <M extends MCLMatrixSlice<M>> Map<SliceId,M> asMatrixSliceSet(RealMatrix m, Configuration conf) {
		
		int nsub = MCLConfigHelper.getNSub(conf);
		int n = m.getRowDimension();
		int num_slices = (n/nsub);
		
		Map<SliceId,M> map = new LinkedHashMap<SliceId, M>();
		
		for(int i = 0; i < num_slices; i++){
			SliceId id = new SliceId();
			id.set(i);
			M slice = MCLContext.<M>getMatrixSliceInstance(conf);
			
			RealMatrix sub_matrix = m.getSubMatrix(0, n-1, i*nsub, (i+1)*nsub -1);
			
			int nnz = getNnz(sub_matrix);
			
			final int[] cols = new int[nnz];
			final long[] rows = new long[nnz];
			final float[] vals = new float[nnz];
			
			RealMatrixPreservingVisitor visitor = new DefaultRealMatrixPreservingVisitor() {
				int i = 0;
				@Override
				public void visit(int row, int column, double value) {
					if(value > 0.0){
						cols[i] = column; rows[i] = row; vals[i++] = (float) value;
					}
				}
			};
			
			sub_matrix.walkInColumnOrder(visitor);
			slice.fill(cols, rows, vals);
			slice.makeStochastic(null);
			map.put(id, slice);
		}
		
		return map;
	}
	
	public static int getKmax(RealMatrix m){
		int kmax = 0;
		
		for(int col = 0, dim = m.getColumnDimension(); col < dim; col++){
			int k = 0;
			double[] val = m.getColumn(col);
			for(int i = 0; i  < val.length;i++){
				if(val[i] > 0.0){
					k++;
				}
			}
			kmax = Math.max(kmax, k);
		}
		
		return kmax;
	}
	
	public static int getNnz(RealMatrix m){
		int cnt = 0;
		double[][] data = m.getData();
		
		for(int i = 0; i < data.length;i++){
			for(int j = 0; j<data[i].length;j++){
				if(data[i][j] > 0.0){
					cnt++;
				}
			}
		}
		
		return cnt;
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
			for(SliceEntry e : slice.dump()) {
				o.setEntry((int) e.row, e.col+ (id.get()*nsub), e.val);
			}
		}
		
		return o;
	}
	
	public static double absMax(RealMatrix m){
		double max = 0.0f;
		double [][] data = m.getData();
		for(int i = 0;i < data.length;i++){
			for(int j = 0; j < data[i].length; j++){
				max = Math.max(max, Math.abs(data[i][j]));
			}
		}
		return max;
	}
	
	public static double sum(RealMatrix m){
		double sum = 0.0f;
		double [][] data = m.getData();
		for(int i = 0;i < data.length;i++){
			for(int j = 0; j < data[i].length; j++){
				sum += Math.abs(data[i][j]);
			}
		}
		return sum;
	}
}
