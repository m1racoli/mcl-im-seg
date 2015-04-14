/**
 * 
 */
package io.writables;

/**
 * @author Cedrik
 *
 */
public class TestCSCSlice extends TestMCLMatrixSlice<CSCSlice> {

	@Override
	protected Class<CSCSlice> getSliceClass() {
		return CSCSlice.class;
	}

}
