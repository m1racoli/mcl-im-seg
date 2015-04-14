/**
 * 
 */
package transform;

/**
 * a transformation, which also has an inverse
 * 
 * @author Cedrik
 *
 */
public interface Transformation<S,D> {

	public D get(S val);
	
	public S inv(Object val);
	
}
