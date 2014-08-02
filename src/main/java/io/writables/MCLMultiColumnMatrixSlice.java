/**
 * 
 */
package io.writables;

import org.apache.hadoop.io.Writable;

/**
 * @author Cedrik
 *
 */
@SuppressWarnings("rawtypes")
public abstract class MCLMultiColumnMatrixSlice<V extends MCLMultiColumnMatrixSlice, E extends Writable> extends MCLMatrixSlice<V,E> {

	public abstract void insert(long row, long col, float val);
	
}
