/**
 * 
 */
package io.matrix;

/**
 * @author Cedrik
 *
 */
public abstract class MCLMatrix implements Iterable<MatrixEntry> {
	
	public void fill(Iterable<MatrixEntry> entries){
		throw new UnsupportedOperationException();
	}
	
	public void mclStep(){
		throw new UnsupportedOperationException();
	}
	
	public void rmclStep(){
		throw new UnsupportedOperationException();
	}
	
	public void transpose(){
		throw new UnsupportedOperationException();
	}

}
