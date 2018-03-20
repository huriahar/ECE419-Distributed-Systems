package app_kvServer;

import ecs.ZKImplementation;
import common.KVConstants;
import org.apache.zookeeper.KeeperException;

public class TimeStamper implements Runnable {
    private ZKImplementation zkImplServer;
    private String zkPath;
    private boolean isRunning = false;

    public TimeStamper(ZKImplementation zkImplServer, String zkPath) {
        this.zkImplServer = zkImplServer;
        this.zkPath = zkPath;
        this.isRunning = true;
    }

    @Override
    public void run() {
        while(isRunning) {
            updateTimeStamp();
            try {
                Thread.sleep(KVConstants.TIMESTAMPER_SLEEP_TIME);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }
    }

    private void updateTimeStamp() {
        //Update timestamp on the server's Znode
        try {
            String data = zkImplServer.readData(this.zkPath);         
            String[] info = data.split(KVConstants.SPLIT_DELIM);
            info[3] = getCurrentTimeString();
            data = String.join(KVConstants.DELIM, info);
            zkImplServer.updateData(this.zkPath, data);
        } catch (KeeperException e) {
            System.out.println("ERROR: Unable to update ZK " + e);
            stop();
        } catch (InterruptedException e) {
            System.out.println("ERROR: ZK Interrupted" + e);
            stop();
        }
    }

    private String getCurrentTimeString() {
        String ret = Long.toString(System.currentTimeMillis());
        //System.out.println("updating kvserver's timestamp.... " + ret);
        return ret;
    }

    public void stop() {
        this.isRunning = false;
    }


} 
