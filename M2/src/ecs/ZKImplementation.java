package ecs;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import common.*;

public class ZKImplementation implements Watcher{
	private ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	
	private static Logger logger = Logger.getRootLogger();
	
	public void zkConnect (String host)
			throws IOException, InterruptedException {
		zk = new ZooKeeper(host, KVConstants.SESSION_TIMEOUT, this);
		connectedSignal.await();
	}
	
	@Override
	public void process (WatchedEvent event) {	// Watcher interface
		if (event.getState() == KeeperState.SyncConnected) {
			connectedSignal.countDown();
		}
	}
	
	public void createGroup (String groupName)
			throws KeeperException, InterruptedException {
		String path = KVConstants.ZK_SEP + groupName;
		String createdPath = zk.create(path,  null,  Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		logger.info("Created group " + createdPath);
	}
	
	public void close()
			throws InterruptedException {
		zk.close();
	}
	
	public void list (String groupName)
			throws KeeperException, InterruptedException {
		String path = KVConstants.ZK_SEP + groupName;
		try {
			List<String> children = zk.getChildren(path, false);
			if (children.isEmpty()) {
				logger.info("No members in group " + groupName);
			}
			else {
				for (String child : children) {
					System.out.println(child);
				}
			}
		}
		catch (KeeperException.NoNodeException e) {
			logger.error("Group " + groupName + " does not exist");
		}
	}
	
	public void joinGroup (String groupName, String memberName)
			throws KeeperException, InterruptedException {
		String path = KVConstants.ZK_SEP + groupName + KVConstants.ZK_SEP + memberName;
		// When a server joins the group, it is in SERVER_STOPPED state - save that in znode
		String createdPath = zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		logger.info("Added node " + createdPath);
	}
	
	public void updateData (String path, String data)
			throws KeeperException, InterruptedException {
		Stat stat = zk.exists(path, true);
		zk.setData(path, data.getBytes(StandardCharsets.UTF_8), stat.getVersion());
	}
	
	public String readData (String path)
			throws KeeperException, InterruptedException {
		byte[] data = zk.getData(path, true, zk.exists(path, true));
		try {
			return new String(data, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			logger.error("Unable to decode data " + e);
		}
		return null;
	}
	
	public void deleteGroup (String groupName)
			throws KeeperException, InterruptedException {
		String path = KVConstants.ZK_SEP + groupName;
		try {
			List<String> children = zk.getChildren(path, false);
			for (String child : children) {
				zk.delete(path + KVConstants.ZK_SEP + child, -1);
			}
			zk.delete(path, -1);
		}
		catch (KeeperException.NoNodeException e) {
			logger.error("Unable to delete path " + path);
		}
	}
}
