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

	static native void clear(ByteBuffer bb, int nsub);
	
}
