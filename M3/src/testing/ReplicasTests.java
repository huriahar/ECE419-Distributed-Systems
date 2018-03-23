package testing;

import org.junit.Test;

import junit.framework.TestCase;

import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import ecs.*;
import java.util.ArrayList;
import app_kvECS.*;
import java.util.Collection;
import java.math.BigInteger;
import common.*;
import java.lang.Process;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class ReplicasTests extends TestCase {
    
    private ECSClient ecsClient;
    Collection<IECSNode> nodes;

	public void setUp() {
        ecsClient = new ECSClient("testECS.config", "localhost");
        ecsClient.setLevel("INFO");
        try {
            Process p = Runtime.getRuntime().exec(new String[]{"csh","-c","rm -rf SERVER_5000*"});
        } catch (IOException e) {
            System.out.println("could not rm -rf: " + e); 
        }
	}

	public void tearDown() {
        ecsClient.shutdown();
        ecsClient.disconnect();
	}

    // ------------------------------ helper functions for tests --------------------------------//
    public void connectToKVServer(KVStore kvClient) {
        Exception ex = null;
        try {
            kvClient.connect();
        } catch (Exception e) {
            System.out.println("ERROR when connecting to client");
            ex = e;
        }
        assertTrue(ex == null);
    }

    public void getKVPair(KVStore kvClient, String key, String value, StatusType expectedStatus) {
        KVMessage response = null;
        Exception ex = null;
        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            System.out.println("ERROR when getting key");
            ex = e;
        }
        assertTrue(response != null && response.getStatus().equals(expectedStatus)
        && response.getValue().equals(value));
    }

    public void putKVPair(KVStore kvClient, String key, String value, StatusType expectedStatus) {
        KVMessage response = null;
        Exception ex = null;
        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            System.out.println("ERROR when putting key");
            ex = e;
        }
        assertTrue(response != null && response.getStatus().equals(expectedStatus));
    }

    // ------------------------------ tests start here -----------------------------------------//

    @Test
    public void testDetectServerCrash() {
        System.out.println("********** In 1 Test **************");
        nodes = ecsClient.addNodes(1, "LRU", 5);
        IECSNode node1 = nodes.iterator().next();
        Exception ex = null;
        try {
            Runtime run = Runtime.getRuntime();
            //kill_knserver.py finds this pattern with the given port and kills the process
            //java     11515 elsaye10   23u  IPv6 547623      0t0  TCP *:50005 (LISTEN)
            String[] launchCmd = {"python", "kill_kvserver.py", Integer.toString(node1.getNodePort())};
            Process proc;
            proc = run.exec(launchCmd);
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            ex = e;
        } catch (IOException e) {
            ex = e;
        }
        assertTrue(ex == null);
        boolean nocrashes = ecsClient.checkServerStatus();
        assertFalse(nocrashes);
   }

}

