/**
 * 
 */
package io.writables.nat;

import io.test.NativeTest;
import java.nio.ByteBuffer;

import mapred.MCLStats;

/**
 * helper class of native methods
 * 
 * @author Cedrik
 *
 */
final class NativeCSCSliceHelper {
	
	static {
		try {
			System.loadLibrary(NativeTest.NATIVE_TEST_LIB_NAME);
		} catch (Throwable e){
			System.err.printf("could not load library '%s' "
					+ "from java.library.path: %s\n", NativeTest.NATIVE_TEST_LIB_NAME,System.getProperty("java.library.path"));
			throw e;
		}
	}
	
	static native void setParams(int nsub, int select, boolean auto_prune,
			double inflation, float cutoff, float pruneA, float pruneB, int kmax);

	static native void clear(ByteBuffer bb);
	
	/**
	 * add b2 to b1
	 * @param b1
	 * @param b2
	 */
	static native boolean add(ByteBuffer b1, ByteBuffer b2);
	
	static native boolean equals(ByteBuffer b1, ByteBuffer b2);
	
	static native double sumSquaredDifferences(ByteBuffer b1, ByteBuffer b2);

	static native void addLoops(ByteBuffer bb, int id);

	static native void makeStochastic(ByteBuffer bb);

	static native void inflateAndPrune(ByteBuffer bb, MCLStats stats);

	static native int size(ByteBuffer bb);
	
	/**
	 * starts iteration over the blocks in a slice
	 * 
	 * @param bb matrix slice to iterate over
	 * @return block of matrix slice or null if islice 
	 */
	static native ByteBuffer startIterateBlocks(ByteBuffer bb);
	
	static native ByteBuffer nextBlock();
	
	static native void multiply(ByteBuffer s, ByteBuffer b);
	
}
