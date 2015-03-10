/**
 * 
 */
package io.writables;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mapred.MCLConfigHelper;
import mapred.PrintMatrix;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Cedrik
 *
 */
public abstract class TestMCLMatrixSlice<M extends MCLMatrixSlice<M>> {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {}
	
	public static final <M extends MCLMatrixSlice<M>> void fill(M m, int[] col, long[] row, float[] val) {
		
		int l = col.length;
		
		if(l != row.length || l != val.length) {
			throw new IllegalArgumentException("dimension missmatch of input arrays col,row,val");
		}
		
		List<SliceEntry> entries = new ArrayList<SliceEntry>(l);
		for(int i = 0; i < l; i++) {
			entries.add(SliceEntry.get(col[i], row[i], val[i]));
		}
		Collections.sort(entries);
		
		m.fill(entries);
	}
	
	public M getInstance(long n, int kmax, int nsub){
		Configuration conf = new Configuration();
		MCLConfigHelper.setPrintMatrix(conf, PrintMatrix.ALL);
		MCLConfigHelper.setN(conf, n);
		MCLConfigHelper.setNSub(conf, nsub);
		MCLConfigHelper.setKMax(conf, kmax);
		MCLConfigHelper.setMatrixSliceClass(conf, getSliceClass());
		return MCLMatrixSlice.<M>getInstance(conf);
	}
	
	/**
	 * Test method for 
	 * {@link io.writables.MCLMatrixSlice#fill(java.lang.Iterable)}, 
	 * {@link io.writables.MCLMatrixSlice#dump()
	 */
	@Test
	public final void testInit() {
		M m = getInstance(9, 6, 3);
		int[]	col = new int[]{0,0,0,1,1,1,1,1,2,2};
		long[]	row = new long[]{1,2,4,1,2,3,6,7,5,8};
		float[]	val = new float[]{0.5f,0.1f,0.4f,0.6f,0.2f,0.6f,0.1f,0.7f,0.4f,0.2f};
		fill(m, col, row, val);
		
		assertEquals("size does not match", 10, m.size());
		
		{
			int i = 0;
			for(SliceEntry e : m.dump()){
				assertEquals("dump of entry " + i + " doesn't match", SliceEntry.get(col[i], row[i], val[i]), e);
				i++;
			}
			assertEquals("iteration not complete", col.length, i);
		}
		
		m.clear();
		
		assertTrue("slice should be empty", m.isEmpty());
		
	}
	
	@Test
	public final void testEquals(){
		M m1 = getInstance(9, 6, 3);
		M m2 = getInstance(9, 6, 3);
		int[]	col = new int[]{0,0,0,1,1,1,1,1,2,2};
		long[]	row = new long[]{1,2,4,1,2,3,6,7,5,8};
		float[]	val = new float[]{0.1f,0.2f,0.3f,0.6f,0.2f,0.6f,0.1f,0.7f,0.4f,0.2f};
		fill(m1, col, row, val);
		fill(m2, col, row, val);
		
		assertEquals("slices are not equal", m2, m1);
	}
	
	@Test
	public final void testDeepCopy(){
		M m1 = getInstance(9, 6, 3);
		int[]	col = new int[]{0,0,0,1,1,1,1,1,2,2};
		long[]	row = new long[]{1,2,4,1,2,3,6,7,5,8};
		float[]	val = new float[]{0.5f,0.1f,0.4f,0.6f,0.2f,0.6f,0.1f,0.7f,0.4f,0.2f};
		fill(m1, col, row, val);
		
		M m2 = m1.deepCopy();
		
		assertNotSame("slices are same", m1, m2);
		
		assertEquals("alices are not equal", m1, m2);
	}
	
	@Test
	public final void testBlockIterator1(){
		M m1 = getInstance(9, 6, 3);
		int[]	col =  new int[]{0,0,0,1,1,1,1,1,2,2};
		long[]	row = new long[]{1,2,4,1,2,3,6,7,5,8};
		float[]	val = new float[]{0.1f,0.2f,0.3f,0.4f,0.5f,0.6f,0.7f,0.8f,0.9f,1.0f};
		fill(m1, col, row, val);
		
		int[] ids = new int[]{0,1,2};
		int[] sizes = new int[]{4,3,3};
		int i = 0;
		SliceId id = new SliceId();
		for(M m : m1.getSubBlocks(id)){
			int m_size = m.size();
			M s = m.deepCopy();
			
			assertEquals("copy of subblock doesnt have same size", m_size, s.size());
			assertEquals("id missmatch", ids[i], id.get());
			assertEquals("size missmatch", sizes[i], m_size);
			i++;
		}
		
		assertEquals("iteration not complete", ids.length, i);
	}
	
	@Test
	public final void testBlockIterator2(){
		M m1 = getInstance(9, 6, 3);
		int[]	col =  new int[]{0,0,0,1,1,1,1,2,2,2};
		long[]	row = new long[]{1,2,4,1,2,3,4,0,1,2};
		float[]	val = new float[]{0.1f,0.2f,0.3f,0.4f,0.5f,0.6f,0.7f,0.8f,0.9f,1.0f};
		fill(m1, col, row, val);
		
		int[] ids = new int[]{0,1};
		int[] sizes = new int[]{7,3};
		int i = 0;
		SliceId id = new SliceId();
		for(M m : m1.getSubBlocks(id)){
			int m_size = m.size();
			M s = m.deepCopy();
			
			assertEquals("copy of subblock doesnt have same size", m_size, s.size());
			assertEquals("id missmatch", ids[i], id.get());
			assertEquals("size missmatch", sizes[i], m_size);
			i++;
		}
		
		assertEquals("iteration not complete", ids.length, i);
	}
	
	@Test
	public final void testDoubleInserts(){
		M m1 = getInstance(9, 6, 3);
		int[]	col1 =  new int[]{0,0,0,1,1,1,1,1,2,2};
		long[]	row1 = new long[]{1,2,4,1,2,3,6,7,5,8};
		float[]	val1 = new float[]{0.1f,0.2f,0.3f,0.4f,0.5f,0.6f,0.7f,0.8f,0.9f,1.0f};
		fill(m1, col1, row1, val1);
		
		M m2 = getInstance(9, 6, 3);
		int[]	col2 =  new int[]{0,0,0,0,1,1,1,1,1,1,2,2,2};
		long[]	row2 = new long[]{1,2,2,4,1,2,3,3,6,7,5,8,8};
		float[]	val2 = new float[]{0.1f,0.2f,-1,0.3f,0.4f,0.5f,0.6f,1,0.7f,0.8f,0.9f,1.0f,1};
		fill(m2, col2, row2, val2);
		
		assertEquals("slices are not same", m1, m2);
		
	}
	
	@Test
	public final void squaredDiff0(){
		M m1 = getInstance(9, 6, 3);
		int[]	col1 =  new int[]{0,0,0,1,1,1,1,1,2,2};
		long[]	row1 = new long[]{1,2,4,1,2,3,6,7,5,8};
		float[]	val1 = new float[]{0.1f,0.2f,0.3f,0.4f,0.5f,0.6f,0.7f,0.8f,0.9f,1.0f};
		fill(m1, col1, row1, val1);
		
		M m2 = getInstance(9, 6, 3);
		int[]	col2 =  new int[]{0,0,0,0,1,1,1,1,1,1,2,2,2};
		long[]	row2 = new long[]{1,2,2,4,1,2,3,3,6,7,5,8,8};
		float[]	val2 = new float[]{0.1f,0.2f,-1,0.3f,0.4f,0.5f,0.6f,1,0.7f,0.8f,0.9f,1.0f,1};
		fill(m2, col2, row2, val2);
		
		assertEquals("slices are not same", m1, m2);
		assertEquals("diff not zero", 0.0, m1.sumSquaredDifferences(m2), 0.0);
	}
	
	@Test
	public final void addition1(){
		M m1 = getInstance(9, 6, 3);
		int[]	col1 =  new int[]{0,0,0,1,1,1,1,1,2,2};
		long[]	row1 = new long[]{1,2,4,1,2,3,6,7,5,8};
		float[]	val1 = new float[]{0.1f,0.2f,0.3f,0.4f,0.5f,0.6f,0.7f,0.8f,0.9f,1.0f};
		fill(m1, col1, row1, val1);
		M m2 = m1.getInstance();
		
		m2.add(m1.deepCopy());
		
		assertEquals("slices are not equal", m1, m2);
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {}

	protected abstract Class<M> getSliceClass();

}
