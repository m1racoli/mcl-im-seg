/**
 * 
 */
package io.writables.nat;

import io.test.NativeTest;

import java.nio.ByteBuffer;

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
	
	static native void setNsub(int nsub);

	static native void clear(ByteBuffer bb);
	
	/**
	 * add b2 to b1
	 * @param b1
	 * @param b2
	 */
	static native void add(ByteBuffer b1, ByteBuffer b2);
	
	static native boolean equals(ByteBuffer b1, ByteBuffer b2);
	
	static native double sumSquaredDifferences(ByteBuffer b1, ByteBuffer b2);
	
}
