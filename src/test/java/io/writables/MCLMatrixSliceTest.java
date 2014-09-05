package io.writables;

import static org.junit.Assert.*;
import io.writables.MCLMatrixSlice.MatrixEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import mapred.MCLConfigHelper;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public abstract class MCLMatrixSliceTest<M extends MCLMatrixSlice<M>> {
	
	private final int n = 100;
	private final int k = 100;
	private final int nsub = 50;
	private final long seed = 34531423L;
	private final double dense = 0.8;
	
	public static final List<MatrixEntry> getRandom(int n, int nsub, long seed, double dense){
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
	
	public final List<MatrixEntry> getUnity(){
		return getUnity(n);
	}
	
	public static final List<MatrixEntry> getUnity(int n){
		return getDiagonal(n, 1.0f);
	}
	
	public static final List<MatrixEntry> getDiagonal(int n, float d){
		List<MatrixEntry> list = new ArrayList<MCLMatrixSlice.MatrixEntry>();
		for(int i = 0; i < n; i++){
			list.add(MatrixEntry.get(i, i, d));
		}
		return list;
	}
	
	public Configuration getConf(int n, int nsub){
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
		return getPreparedInstance(n,nsub,getRandom());
	}
	
	public M getPreparedInstance(int n, int nsub, Iterable<MatrixEntry> entries) {
		M m = MCLMatrixSlice.getInstance(getConf(n,nsub));
		m.fill(entries);
		return m;
	}
	
	@Test
	public void testFillDump() {
		M m = MCLMatrixSlice.getInstance(getConf(n,nsub));
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
		List<MatrixEntry> list = getRandom();
		M m = getPreparedInstance(n, nsub, list);
		assertTrue(m.size() == list.size());
	}
	
	@Test
	public void testRefill() {
		M m1 = getPrepadedInstance();
		M m2 = MCLMatrixSlice.getInstance(getConf(n,nsub));
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
	
	@Test
	public void testUnitMultRight() {
		M m1 = getPreparedInstance(n,nsub,getRandom());
		M m2 = getPreparedInstance(nsub,nsub,getUnity(nsub));
		M result = m2.multipliedBy(m1, null);
		assertEquals(m1, result);
	}

	@Test
	public void testUnitMultLeft() {
		M m1 = getPreparedInstance(n,nsub,getRandom());
		M m2 = getPreparedInstance(n,n,getUnity(n));
		M result = m1.multipliedBy(m2, null);
		assertEquals(m1, result);
	}
	
	@Test
	public void testMakeUnitStochastic() {
		M m1 = getPreparedInstance(n,n,getUnity(n));
		M result = m1.deepCopy();
		result.makeStochastic(null);
		assertEquals(m1, result);
	}
	
	@Test
	public void testSubBLockIterator() {
		M m = getPreparedInstance(n,nsub,getUnity(nsub));
		SliceId id = new SliceId();		
		for(M s : m.getSubBlocks(id)){
			switch(id.get()){
			case 0:
				assertEquals(s.deepCopy(), getPreparedInstance(nsub,nsub,getUnity(nsub)));
				break;
			case 1:
				assertEquals(s.deepCopy(), getPreparedInstance(nsub,nsub,Collections.<MatrixEntry>emptyList()));
				break;
			default:
				fail();
			}
		}
	}
	
	@Test
	public void testDiagAdd(){
		M m2 = getPreparedInstance(n, n, getDiagonal(n, 3.0f));
		M m4 = getPreparedInstance(n, n, getDiagonal(n, 4.5f));
		M m6 = getPreparedInstance(n, n, getDiagonal(n, 7.5f));
		m2.add(m4);
		assertEquals(m2, m6);		
	}
	
	@Test
	public void testAddZero(){
		M m = getPrepadedInstance();
		M z = getPreparedInstance(n, nsub, Collections.<MatrixEntry>emptyList());
		M rerult = m.deepCopy();
		rerult.add(z);
		assertEquals(m, rerult);		
	}
	
	@Test
	public void testZeroAdd(){
		M m = getPrepadedInstance();
		M z = getPreparedInstance(n, nsub, Collections.<MatrixEntry>emptyList());
		z.add(m);
		assertEquals(m, z);		
	}
	
	@Test
	public void testDiagMultAndNormalize(){
		M m = getPrepadedInstance();
		m.makeStochastic(null);
		M d = getPreparedInstance(nsub, nsub, getDiagonal(nsub, (float) Math.PI));
		M result = d.multipliedBy(m, null);
		result.makeStochastic(null);
		assertEquals(0.0f, maxDiff(n, nsub, m, result),1.0e-8f);
	}
	
	public float maxDiff(int n, int nsub, M m1, M m2) {
		float[][] diff = new float[n][nsub];
		
		for(MatrixEntry e : m1.dump()){
			diff[(int) e.row][e.col] = e.val;
		}
		for(MatrixEntry e : m2.dump()){
			diff[(int) e.row][e.col] -= e.val;
		}
		float max = 0.0f;
		for(int i = 0; i < n; i++){
			for(int j = 0; j < nsub; j++){
				max = Math.max(diff[i][j], max);
			}
		}
		return max;
	}
}
