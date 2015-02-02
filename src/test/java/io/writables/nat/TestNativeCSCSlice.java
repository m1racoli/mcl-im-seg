/**
 * 
 */
package io.writables.nat;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Cedrik
 *
 */
public class TestNativeCSCSlice {
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link io.writables.nat.NativeCSCSliceHelper#clear(java.nio.ByteBuffer, int)}.
	 */
	@Test
	public void testClear() {
		int nsub = 128;
		ByteBuffer bb = getBBInstance(nsub);
		NativeCSCSliceHelper.setNsub(nsub);
		NativeCSCSliceHelper.clear(bb);
		IntBuffer ibb = bb.asIntBuffer();
		for(int i = 0; i < nsub; i++){
			assertEquals("buffer is not 0 at position "+i, 0, ibb.get());
		}
	}
	
	private ByteBuffer getBBInstance(int nsub){
		ByteBuffer bb = ByteBuffer.allocateDirect(nsub * Integer.SIZE);
		bb.order(ByteOrder.nativeOrder());
		fillIntBuffer(bb.asIntBuffer(), 3141);
		return bb;
	}
	
	private static void fillIntBuffer(IntBuffer bb, long seed){
		final Random rand = new Random(seed);
		for(int i = 0; i < bb.capacity(); i++){
			bb.put(i, rand.nextInt());
		}
	}

}
