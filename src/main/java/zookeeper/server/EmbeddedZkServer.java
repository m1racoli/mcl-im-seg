/**
 * 
 */
package zookeeper.server;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Cedrik
 *
 */
public class EmbeddedZkServer {

	private static ZkServerThread serverThread = null;
	
	public static void init(Configuration conf)
	{	
		if(serverThread == null){
			serverThread = ZkServerThread.fromConfig(conf);
			serverThread.start();
		}
	}
}
