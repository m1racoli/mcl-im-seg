/**
 * 
 */
package io.writables.nat;

import io.writables.TestMCLMatrixSlice;

/**
 * @author Cedrik
 *
 */
public class TestNativeCSCSlice extends TestMCLMatrixSlice<NativeCSCSlice>  {

	@Override
	protected Class<NativeCSCSlice> getSliceClass() {
		return NativeCSCSlice.class;
	}

}
