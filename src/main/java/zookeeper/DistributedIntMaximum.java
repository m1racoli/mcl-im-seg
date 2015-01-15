/**
 * 
 */
package zookeeper;

/**
 * @author Cedrik
 *
 */
public final class DistributedIntMaximum extends DistributedInt {

	@Override
	protected int merge(int v1, int v2) {
		return Math.max(v1, v2);
	}

}
