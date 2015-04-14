/**
 * 
 */
package zookeeper;

/**
 * @author Cedrik
 *
 */
public final class DistributedLongMaximum extends DistributedLong {

	@Override
	protected long merge(long v1, long v2) {
		return Math.max(v1, v2);
	}

}
