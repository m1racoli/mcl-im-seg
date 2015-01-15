/**
 * 
 */
package transform;

/**
 * @author Cedrik
 *
 */
public interface Transformation<S,D> {

	public D get(S val);
	
	public S inv(Object val);
	
}
