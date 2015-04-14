/**
 * 
 */
package zookeeper;

/**
 * @author Cedrik
 *
 */
public class ZkMetricOperationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6903461021981690951L;

	public ZkMetricOperationException() {
		super();
	}

	public ZkMetricOperationException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ZkMetricOperationException(String message, Throwable cause) {
		super(message, cause);
	}

	public ZkMetricOperationException(String message) {
		super(message);
	}

	public ZkMetricOperationException(Throwable cause) {
		super(cause);
	}

	

}
