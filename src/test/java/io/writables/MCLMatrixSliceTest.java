package io.writables;

import static org.junit.Assert.*;
import io.writables.MCLMatrixSlice.MatrixEntry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import mapred.MCLConfigHelper;
import mapred.PrintMatrix;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public abstract class MCLMatrixSliceTest<M extends MCLMatrixSlice<M>> {
	
	private final long n = 100;
	private final int k = 100;
	private final int nsub = 50;
	private final long seed = 34531423L;
	private final double dense = 0.8;
	
	public static final List<MatrixEntry> getRandom(long n, int nsub, long seed, double dense){
		Random random = new Random(seed);
		List<MatrixEntry> entries = new ArrayList<MatrixEntry>();

		for(int col = 0; col < nsub; col++){
			for(int row = 0; row < n; row++){			
				if(random.nextDouble() < dense){
					entries.add(MatrixEntry.get(col, row, random.nextFloat()));
				}
			}
		}
		
		return entries;
	}
	
	public final List<MatrixEntry> getRandom(){
		return getRandom(n, nsub, seed, dense);
	}
	
	public Configuration getConf(){
		Configuration conf = new Configuration();
		MCLConfigHelper.setN(conf, n);
		MCLConfigHelper.setKMax(conf, k);
		MCLConfigHelper.setNSub(conf, nsub);
		MCLConfigHelper.setMatrixSliceClass(conf, getTestClass());
		return conf;
	}
	
	/** 
	 * @return instance to test;
	 */
	abstract Class<M> getTestClass();
	
	public M getPrepadedInstance(){
		M m = MCLMatrixSlice.getInstance(getConf());
		m.fill(getRandom());
		return m;
	}
	
	@Test
	public void testFillDump() {
		M m = MCLMatrixSlice.getInstance(getConf());
		List<MatrixEntry> list = getRandom();
		Iterator<MatrixEntry> iter = list.iterator();
		m.fill(list);
		for(MatrixEntry e : m.dump()){
			assertTrue(iter.hasNext());
			assertEquals(e, iter.next());
		}
		assertFalse(iter.hasNext());
	}
	
	@Test
	public void testSize(){
		M m = MCLMatrixSlice.getInstance(getConf());
		List<MatrixEntry> list = getRandom();
		m.fill(list);
		assertTrue(m.size() == list.size());
	}
	
	@Test
	public void testRefill() {
		M m1 = getPrepadedInstance();
		M m2 = MCLMatrixSlice.getInstance(getConf());
		m2.fill(m1.dump());
		assertEquals(m1, m2);
		m2.fill(m1.dump());
		assertEquals(m1, m2);
	}

	@Test
	public void testDeepCopy() {
		M m1 = getPrepadedInstance();
		M m2 = m1.deepCopy();
		assertNotSame(m1, m2);
		assertEquals(m1, m2);
	}
	
	//TODO normalization

}
