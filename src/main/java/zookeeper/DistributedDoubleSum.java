/**
 * 
 */
package zookeeper;

/**
 * @author Cedrik
 *
 */
public class DistributedDoubleSum extends DistributedDouble {

	/* (non-Javadoc)
	 * @see zookeeper.DistributedFloat#merge(float, float)
	 */
	@Override
	protected double merge(double v1, double v2) {
		return v1+v2;
	}

}
