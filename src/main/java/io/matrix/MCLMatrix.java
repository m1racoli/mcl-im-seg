/**
 * 
 */
package io.matrix;

/**
 * @author Cedrik
 *
 */
public abstract class MCLMatrix {
	
	public abstract void fill(Iterable<MatrixEntry> entries);
	
	public abstract void mclStep();
	
	public abstract void rmclStep();
	
	public abstract void transpose();
	
	public abstract Iterable<MatrixEntry> dump();

}
