package zookeeper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import mapred.MCLConfigHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkMetric {

	private static final Logger logger = LoggerFactory.getLogger(ZkMetric.class);
	
	public static final String ZK_METRIC_PATH_CONF = "zk.metric.path";
	public static final String ZK_METRIC_SESSION_TIMEOUT_CONF = "zk.metric.session.timeout";
	
	private static final Set<String> paths = Collections.synchronizedSet(new LinkedHashSet<String>());
	private static ZooKeeper zk = null;
	
	private static final ZooKeeper getZookeeperInstance(Configuration conf) throws IOException, InterruptedException{
		
		if(zk == null){
			synchronized (ZkMetric.class) {
				String hosts = MCLConfigHelper.getZkHosts(conf);
				String path = conf.get(ZK_METRIC_PATH_CONF,"");
				int sessionTimeout = conf.getInt(ZK_METRIC_SESSION_TIMEOUT_CONF, 10000);
				final CountDownLatch lock = new CountDownLatch(1);
				final Watcher watcher = new Watcher(){
					@Override
					public void process(WatchedEvent event) {
						if(event.getState() == KeeperState.SyncConnected)
							lock.countDown();
					}			
				};
				zk = new ZooKeeper(hosts+path, sessionTimeout, watcher);
				if(!lock.await(sessionTimeout, TimeUnit.SECONDS)){
					throw new IOException("zookeeper connect took to long");
				}
			}
		}
		
		return zk;
	}
	
	public static final void init(Configuration conf, String path, boolean override) throws InterruptedException, ZkMetricOperationException, IOException {
		
		if(path == null || path.isEmpty()) {
			logger.warn("metric path not set. value = {}");
		}
		
		ZooKeeper zk = getZookeeperInstance(conf);
		try {
			if(override && zk.exists(path, false) != null) zk.delete(path, -1);//TODO correct logic
			zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			paths.add(path);
		} catch (KeeperException e) {
			throw new ZkMetricOperationException(e);
		}
	}
	
	public static final <V extends DistributedMetric<?>> V get(Configuration conf, String path) throws ZkMetricOperationException, IOException, InterruptedException{

		if(path == null || path.isEmpty()) {
			logger.warn("metric path not set. value = {}");
		}
		
		try {
			ZooKeeper zk = getZookeeperInstance(conf);
			byte[] data = zk.getData(path, false, null);
			
			if(data == null)
				return null;
			
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
			
			@SuppressWarnings("unchecked")
			Class<V> cls = (Class<V>) Class.forName(in.readUTF());
			final V v = ReflectionUtils.newInstance(cls, conf);
			v.readFields(in);
			logger.debug("loaded metric: {} = {} @ {}",cls.getName(),v.toString(),path);
			return v;
		} catch (KeeperException | ClassNotFoundException e) {
			throw new ZkMetricOperationException(e);
		}
	}

	public static void set(Configuration conf, String path, DistributedMetric<?> m) throws InterruptedException, IOException{
		
		if(path == null || path.isEmpty()) {
			logger.warn("metric path not set. value = {}",m);
			return;
		}
		
		try{
			ZooKeeper zk = getZookeeperInstance(conf);

			final String lock = initLock(zk, path);
			
			final Stat stat = new Stat();
			final byte[] data = zk.getData(path, false, stat);
			final ByteArrayOutputStream byte_out = new ByteArrayOutputStream();
			final DataOutputStream out = new DataOutputStream(byte_out);
			
			if(data != null){
				DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
				String classNamme = in.readUTF();
				@SuppressWarnings("unchecked")
				Class<DistributedMetric<DistributedMetric<?>>> cls = (Class<DistributedMetric<DistributedMetric<?>>>) Class.forName(classNamme);
				final DistributedMetric<DistributedMetric<?>> v = ReflectionUtils.newInstance(cls, conf);
				v.readFields(in);
				v.merge(m);		
				out.writeUTF(classNamme);
				v.write(out);
			} else {
				out.writeUTF(m.getClass().getName());
				m.write(out);
				logger.debug("write new value: {} = {} @ {}",m.getClass(),m,path);
			}
			
			zk.setData(path, byte_out.toByteArray(), stat.getVersion());
			m.clear();
			releaseLock(zk, lock);
			
		} catch (KeeperException | ClassNotFoundException e) {
			throw new IOException(e);
		}
	}
	
	public static final void cleanup(Configuration conf) throws IOException, InterruptedException{
		
		if(paths.isEmpty()){
			close();
			return;
		}
		
		ZooKeeper zk = getZookeeperInstance(conf);
		for(String path : paths)
			zk.delete(path, -1, null, null);
		paths.clear();
		zk.close();
		zk = null;
	}
	
	public static final void close() throws InterruptedException{
		if(zk != null) {
			zk.close();
			zk = null;
		}
	}
	
	private static String initLock(ZooKeeper zk, String path) throws InterruptedException, KeeperException{
		
		final String lockPath = zk.create(path +"/lock", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		final String name = getName(lockPath);
		
		while(true){
			final CountDownLatch lock = new CountDownLatch(1);
			final List<String> nodes = zk.getChildren(path, new Watcher() {				
				@Override
				public void process(WatchedEvent event) {
					if(event.getType() == EventType.NodeChildrenChanged){
						lock.countDown();
					}					
				}
			});
			Collections.sort(nodes);
			final int index = nodes.indexOf(name);
			if(index == 0){
				break;
			}
			if(index == -1){
				throw new RuntimeException("lock must exist in children");
			}
			
			lock.await();
		}
		
		return lockPath;
	}
	
	private static void releaseLock(ZooKeeper zk, String lock) throws InterruptedException, KeeperException{
		zk.delete(lock, -1);
	}
	
	private static final String getName(String path){
		String[] split = path.split("/");
		return split[split.length-1];
	}
	
}
