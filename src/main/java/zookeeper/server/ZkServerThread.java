/**
 * 
 */
package zookeeper.server;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

/**
 * @author Cedrik
 *
 */
public class ZkServerThread extends Thread {
	
	private final ZooKeeperServerMain zooKeeperServer;
	private final ServerConfig configuration;
	
	public static ZkServerThread fromConfig(Configuration conf){
		
		Properties properties = new Properties();
		
		//TODO configuration
		properties.setProperty("dataDir", System.getProperty("java.io.tmpdir"));
		properties.setProperty("tickTime", "2000");
		properties.setProperty("clientPort", "2181");
		properties.setProperty("initLimit", "5");
		properties.setProperty("syncLimit", "2");
		
		return new ZkServerThread(properties);
	}
	
	public ZkServerThread(Properties properties) {
		
		QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
		
		try {
			quorumConfig.parseProperties(properties);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		zooKeeperServer = new ZooKeeperServerMain();		
		configuration = new ServerConfig();
		configuration.readFrom(quorumConfig);
	}
	
	@Override
	public void run() {
		try {
			zooKeeperServer.runFromConfig(configuration);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
