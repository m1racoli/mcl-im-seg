/**
 * 
 */
package zookeeper;

/**
 * @author Cedrik
 *
 */
public class DistributedFloatSum extends DistributedFloat {

	/* (non-Javadoc)
	 * @see zookeeper.DistributedFloat#merge(float, float)
	 */
	@Override
	protected float merge(float v1, float v2) {
		return v1+v2;
	}

}
