/**
 * 
 */
package zookeeper.server;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Cedrik
 *
 */
public class ZkServerThread extends Thread {
	
	private static final Logger logger = LoggerFactory.getLogger(ZkServerThread.class);
	
	private final ZooKeeperServerMain zooKeeperServer;
	private final ServerConfig configuration;
	
	public static ZkServerThread fromConfig(Configuration conf){
		
		Properties properties = new Properties();
		
		//TODO configuration
		properties.setProperty("dataDir", System.getProperty("java.io.tmpdir") + "/" + System.currentTimeMillis());
		properties.setProperty("tickTime", "4000");
		properties.setProperty("clientPort", "2181");
		properties.setProperty("initLimit", "5");
		properties.setProperty("syncLimit", "2");
		
		return new ZkServerThread(properties);
	}
	
	public ZkServerThread(Properties properties) {
		logger.debug("init");
		setDaemon(true);
		Thread parent = Thread.currentThread();
		int parentPrio = parent.getPriority() - 1;
		parentPrio = parentPrio > MIN_PRIORITY ? parentPrio : MIN_PRIORITY;
		parent.setPriority(parentPrio);
		logger.debug("main thread priority set to {}",parent.getPriority());
		logger.debug("ZooKeeper server thread priority: {}",getPriority());
		QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
		
		try {
			quorumConfig.parseProperties(properties);
			File dataDir = new File(quorumConfig.getDataDir());
			dataDir.deleteOnExit();
			logger.debug("dataDir: {}",dataDir);
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new RuntimeException(e);
		}
		
		zooKeeperServer = new ZooKeeperServerMain();		
		configuration = new ServerConfig();
		configuration.readFrom(quorumConfig);
	}
	
	@Override
	public void run() {
		try {
			logger.debug("start");
			zooKeeperServer.runFromConfig(configuration);
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}
}
