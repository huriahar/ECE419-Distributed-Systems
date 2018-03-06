package ecs;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;

import common.*;

public class ZKImplementation implements Watcher {
	private ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	
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
		System.out.println("Created group " + createdPath);
	}
	
	public void close()
			throws InterruptedException {
		zk.close();
	}
}
