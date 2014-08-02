package zookeeper;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public abstract class ZkMetric<V extends Writable> {

	public static final String ZK_METRIC_HOSTS_CONF = "zk.metric.hosts";
	public static final String ZK_METRIC_PATH_CONF = "zk.metric.path";
	public static final String ZK_METRIC_SESSION_TIMEOUT_CONF = "zk.metric.session.timeout";
	
	private final ZooKeeper zk;
	private final CountDownLatch lock = new CountDownLatch(1);
	
	private V cache = null;
	private boolean closed = false;
	
	public ZkMetric(Configuration conf) throws IOException, InterruptedException{
		String hosts = conf.get(ZK_METRIC_HOSTS_CONF,"localhost:2181");
		String path = conf.get(ZK_METRIC_HOSTS_CONF,"");
		int sessionTimeout = conf.getInt(ZK_METRIC_SESSION_TIMEOUT_CONF, 10000);
		zk = new ZooKeeper(hosts+path, sessionTimeout, watcher);
		if(!lock.await(sessionTimeout, TimeUnit.SECONDS)){
			throw new IOException("zookeeper connect took to long");
		}
	}
	
	private final Watcher watcher = new Watcher(){
		@Override
		public void process(WatchedEvent event) {
			switch(event.getState()){
			case SyncConnected:
				lock.countDown();
				break;
			default:
				break;
			}
		}
	};
	
	public void put(V value){
		
		if(closed)
			throw new IllegalStateException("");
		
		if(cache == null){
			
		}
	}
	
	public void submit(String group, String name, final V value) throws InterruptedException, IOException, KeeperException{
		
		
		while(true){
			
			if(cache == null){
				byte[] data = null;
			}
			
			V result = cache == null ? value : merge(cache,value);
			byte[] data = write(result);
			
			try {
				zk.setData(name, data, version);
			} catch (KeeperException e) {
				switch (e.code()) {
				case BADVERSION:
					break;
				case NONODE:
					zk.create(name, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					cache = result;
					version = 0;
					break;
				
				default:
					throw e;
				}
			}
		}
		
		
		
	}
	
	protected abstract V merge(V v1, V v2);
	
	private final byte[] write(Writable v) throws IOException{
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		v.write(new DataOutputStream(out));
		return out.toByteArray();
	}
	
}
