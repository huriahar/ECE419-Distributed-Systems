package testing;

import org.junit.Test;

import junit.framework.TestCase;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    public void readLineFromFile(Path filePath, boolean expectToFind, String value) {
        Exception ex = null;
        ArrayList<String> lines = new ArrayList<String>();
        try {
            lines = new ArrayList<>(Files.readAllLines(filePath, StandardCharsets.UTF_8));
        } catch(IOException e) {
            ex = e;
        }
        if(expectToFind) {
            assertTrue(ex == null);
            if(value == null) {
                assertTrue(lines.isEmpty());
            } else {
                assertTrue(lines.get(0).equals(value));
            }
        } else {
            assertTrue(ex != null);
        }
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

    @Test
    public void testReplicaFileContentTwoNodes() {
        System.out.println("********** In 3 Test **************");
        nodes = ecsClient.addNodes(2, "LRU", 5);
        Iterator<IECSNode> it = nodes.iterator();
        IECSNode node1 = it.next();
        IECSNode node2 = it.next();
        IECSNode temp;
        //Node 1 should contain 50003 - where the key b maps
        if(node1.getNodePort() > node2.getNodePort()) {
            temp = node1;
            node1 = node2;
            node2 = temp;
        }
        assertTrue(ecsClient.start());
        //Connect client to server and put KV pair
        KVStore kvClient = new KVStore("localhost", 50003);
        connectToKVServer(kvClient);
        putKVPair(kvClient, "b", "bb", StatusType.PUT_SUCCESS);
        kvClient.disconnect();


        //Check the content of the coordinator replica files
        String fileName = "SERVER_" + node1.getNodePort(); 
        readLineFromFile(Paths.get(fileName), true, "b|bb");

        fileName = "SERVER_" + node1.getNodePort() + "_PRIMARY"; 
        readLineFromFile(Paths.get(fileName), false, null);

        fileName = "SERVER_" + node2.getNodePort(); 
        readLineFromFile(Paths.get(fileName), true, null);

        fileName = "SERVER_" + node2.getNodePort() + "_PRIMARY"; 
        readLineFromFile(Paths.get(fileName), true, "b|bb");

    }

    @Test
    public void testPersistentDataAfterCrash() {
        System.out.println("********** In 5 Test **************");
        nodes = ecsClient.addNodes(2, "LRU", 5);
        Iterator<IECSNode> it = nodes.iterator();
        IECSNode node1 = it.next();
        IECSNode node2 = it.next();
        assertTrue(ecsClient.start());

        //Connect client to server and put KV pair
        KVStore kvClient = new KVStore("localhost", node1.getNodePort());
        connectToKVServer(kvClient);
        putKVPair(kvClient, "b", "bb", StatusType.PUT_SUCCESS);
        kvClient.disconnect();

        //Kill the server
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

        //Client connects to active server and tries to get the KV pair.
        //Then client connects to the other active server and tries to get the KV
        //pair. One of the servers should be the one from earlier and the other
        //is the crashed server's replacement. Both should contain the KV pait either
        //as a replica or coordinator
        nodes = ecsClient.getNodesLaunched();
        assertTrue(ecsClient.start());
        System.out.println("nodes.size = " + nodes.size());
        it = nodes.iterator();
        node1 = it.next();
        node2 = it.next();
        kvClient = new KVStore("localhost", node1.getNodePort());
        connectToKVServer(kvClient);
        getKVPair(kvClient, "b", "bb", StatusType.GET_SUCCESS);
        kvClient.disconnect();

        //Client reconnects to active server and tries to get the KV pair
        kvClient = new KVStore("localhost", node1.getNodePort());
        connectToKVServer(kvClient);
        getKVPair(kvClient, "b", "bb", StatusType.GET_SUCCESS);
        kvClient.disconnect();
   }
}

