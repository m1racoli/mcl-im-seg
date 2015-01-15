/**
 * 
 */
package zookeeper;

/**
 * @author Cedrik
 *
 */
public class DistributedDoubleMaximum extends DistributedDouble {

	/* (non-Javadoc)
	 * @see zookeeper.DistributedFloat#merge(float, float)
	 */
	@Override
	protected double merge(double v1, double v2) {
		return Math.max(v1, v2);
	}

}
