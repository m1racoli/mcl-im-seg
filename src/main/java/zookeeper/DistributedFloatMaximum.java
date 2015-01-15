/**
 * 
 */
package zookeeper;

/**
 * @author Cedrik
 *
 */
public class DistributedFloatMaximum extends DistributedFloat {

	/* (non-Javadoc)
	 * @see zookeeper.DistributedFloat#merge(float, float)
	 */
	@Override
	protected float merge(float v1, float v2) {
		return Math.max(v1, v2);
	}

}
