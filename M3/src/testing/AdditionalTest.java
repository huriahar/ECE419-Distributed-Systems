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

public class AdditionalTest extends TestCase {
    
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
/*
    @Test
    public void  testInitialization() {
       try{
            System.out.println("********** In 1 Test **************");
            
            nodes = ecsClient.addNodes(1, "LRU", 5);
            assertTrue(ecsClient.start()); 
            //assertTrue(ecsClient.shutdown());
            assertTrue(ecsClient.stop());
        } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 

    @Test
    public void testHashingWithAdd() {
        System.out.println("********** In 2 Test **************");
        BigInteger hash = new BigInteger("0", 16);
        nodes = ecsClient.addNodes(1, "LRU", 5);
        IECSNode init = new ECSNode();
        for(IECSNode node: nodes) {
            init = node;
            hash = md5.encode(node.getNodeHost() + KVConstants.HASH_DELIM + node.getNodePort());
            assertTrue(node.getNodeHashRange()[0].compareTo(hash) == 0);
            assertTrue(node.getNodeHashRange()[1].compareTo(hash) == 0);
            
        }   
        IECSNode newNode = ecsClient.addNode("LRU", 2);
        BigInteger newHash = md5.encode(newNode.getNodeHost() + KVConstants.HASH_DELIM + newNode.getNodePort());
        
        assertTrue(newNode.getNodeHashRange()[1].compareTo(newHash) == 0);
        assertTrue(newNode.getNodeHashRange()[0].compareTo(hash) == 0);
        assertTrue(init.getNodeHashRange()[0].compareTo(newHash) == 0);
    }

    @Test
    public void testRemoveNode() {
        System.out.println("********** In 3 Test **************");
        //This test ensures that, when removing a node from the server,
        //data is transferred over to the existing nodes on the ring
        //network successfully and a client can fetch data from there
        nodes = ecsClient.addNodes(1, "LRU", 5);
        IECSNode init = new ECSNode();
        IECSNode newNode = new ECSNode();
        Collection<String> name = new ArrayList<String>();
 
        for(IECSNode node: nodes) {
            init = node;
            name.add(node.getNodeName());
        }
        ecsClient.start();
        KVStore kvClient = new KVStore("localhost", init.getNodePort());
        connectToKVServer(kvClient);
        putKVPair(kvClient, "a", "1", StatusType.PUT_SUCCESS);
        kvClient.disconnect();
        
        //Add a new node to the system 
        newNode = ecsClient.addNode("LRU", 2);
        assertTrue(newNode != null);

        //Remove the first server
        assertTrue(ecsClient.removeNodes(name));

        //Start servers on the system
        assertTrue(ecsClient.start());

        //Connect to the new server
        kvClient = new KVStore("localhost", newNode.getNodePort());
        connectToKVServer(kvClient);
        //Make sure GET is successful
        getKVPair(kvClient, "a", "1", StatusType.GET_SUCCESS);
    } 

    @Test
    public void testConnectToWrongServer() {
        System.out.println("********** In 4 Test **************");
        // Add a key that hashes to server4, connect to server7
        // make sure you get a "GET_SUCCESS"
        nodes = ecsClient.addNodes(2, "LRU", 5);
        assertTrue(ecsClient.start());
        //connect to server7
        KVStore kvClient = new KVStore("localhost", 50006);
        connectToKVServer(kvClient);
        //key "b" hashes to server4 when only server7 and server4 available
        putKVPair(kvClient, "b", "1", StatusType.PUT_SUCCESS);
        getKVPair(kvClient, "b", "1", StatusType.GET_SUCCESS);
    } 

    @Test
    public void testServerStopped() {
        System.out.println("********** In 5 Test **************");
        nodes = ecsClient.addNodes(1, "FIFO", 5);
        int port = nodes.iterator().next().getNodePort();
        //key hashes to server4 when only server7 and server4 available
        //connect to server7
        KVStore kvClient = new KVStore("localhost", port);
        connectToKVServer(kvClient);
        putKVPair(kvClient, "b", "1", StatusType.SERVER_STOPPED);
        assertTrue(ecsClient.start());
        putKVPair(kvClient, "b", "1", StatusType.PUT_SUCCESS);
    }

    @Test
    public void testWriteLockedPut() {
        System.out.println("********** In 6 Test **************");
        nodes = ecsClient.addNodes(1, "FIFO", 5);
        int port = nodes.iterator().next().getNodePort();
        String name = nodes.iterator().next().getNodeName();
        assertTrue(ecsClient.start());

        //key hashes to server4 when only server7 and server4 available
        //connect to server7
        KVStore kvClient = new KVStore("localhost", port);
        connectToKVServer(kvClient);
        putKVPair(kvClient, "b", "1", StatusType.PUT_SUCCESS);

        assertTrue(ecsClient.lock(name));
        putKVPair(kvClient, "b", "2", StatusType.SERVER_WRITE_LOCK);

        assertTrue(ecsClient.unlock(name));
        putKVPair(kvClient, "b", "2", StatusType.PUT_UPDATE);
    }

    @Test
    public void testWriteLockedGet() {
        System.out.println("********** In 7 Test **************");
        nodes = ecsClient.addNodes(1, "FIFO", 5);
        int port = nodes.iterator().next().getNodePort();
        String name = nodes.iterator().next().getNodeName();
        assertTrue(ecsClient.start());

        //key hashes to server4 when only server7 and server4 available
        //connect to server7
        KVStore kvClient = new KVStore("localhost", port);
        connectToKVServer(kvClient);
        putKVPair(kvClient, "b", "1", StatusType.PUT_SUCCESS);

        assertTrue(ecsClient.lock(name));
        getKVPair(kvClient, "b", "1", StatusType.GET_SUCCESS);
    }

    @Test
    public void testInitTooManyServers() {
        System.out.println("********** In 8 Test **************");
        nodes = ecsClient.addNodes(25, "FIFO", 5);
        assertTrue(nodes.size() == 0);
    }

    @Test
    public void testMultipleClients() {
        System.out.println("********** In 9 Test **************");
        //Test that multiple clients can interact with a single server
        IECSNode fifoNode = (ecsClient.addNodes(1, "FIFO", 2)).iterator().next();
        KVStore kvClient1 = new KVStore("localhost", fifoNode.getNodePort());
        KVStore kvClient2 = new KVStore("localhost", fifoNode.getNodePort());
        connectToKVServer(kvClient1);
        connectToKVServer(kvClient2);
        assertTrue(ecsClient.start());
        putKVPair(kvClient1, "a", "aa", StatusType.PUT_SUCCESS);
        getKVPair(kvClient2, "a", "aa", StatusType.GET_SUCCESS);
    }

//    @Test
//    public void testVerifyCache() {
//        System.out.println("********** In 10 Test **************");
//        //Add two servers, one with a FIFO and one with an LRU cache
//        IECSNode fifoNode = (ecsClient.addNodes(1, "FIFO", 2)).iterator().next();
//        assertTrue(fifoNode != null);
//        IECSNode lruNode = ecsClient.addNode("LRU", 2);
//        assertTrue(lruNode != null);
//        assertTrue(ecsClient.start());
//        //Connect to the FIFO server
//        KVStore kvClient = new KVStore("localhost", fifoNode.getNodePort());
//        connectToKVServer(kvClient);
//        //Put 3 KV pair, expect the first to be evicted
//        putKVPair(kvClient, "a", "aa", StatusType.PUT_SUCCESS);
//        putKVPair(kvClient, "b", "bb", StatusType.PUT_SUCCESS);
//        putKVPair(kvClient, "c", "cc", StatusType.PUT_SUCCESS);
//        //Check is inCache "a", expect to get false
//        //TODO incomplete test! how do we test caching if we dont have access to kvserver?
//    }

    @Test
    public void testHashingWithRemove() {
        System.out.println("********** In 11 Test **************");
        nodes = ecsClient.addNodes(2, "LRU", 5);
        Iterator<IECSNode> it = nodes.iterator();
        IECSNode node1 = it.next();
        IECSNode node2 = it.next();
        assertTrue(node1 != null && node2 != null);
        assertTrue(node1 != node2);

        assertTrue(node1.getNodeHashRange()[1].compareTo(node2.getNodeHashRange()[0]) == 0);
        assertTrue(node1.getNodeHashRange()[0].compareTo(node2.getNodeHashRange()[1]) == 0);

        Collection<String> names = new ArrayList<String>();
        names.add(node1.getNodeName());
        assertTrue(ecsClient.removeNodes(names));

        BigInteger hash = new BigInteger("0", 16);
        hash = md5.encode(node2.getNodeHost() + KVConstants.HASH_DELIM + node2.getNodePort());
        assertTrue(node2.getNodeHashRange()[0].compareTo(hash) == 0);
        assertTrue(node2.getNodeHashRange()[1].compareTo(hash) == 0);
    }


    @Test
    public void testPersistentStorage() {
        //Test whether data persists after all nodes are removed from the ring
        //network then nodes are added again
        System.out.println("********** In 12 Test **************");
        nodes = ecsClient.addNodes(2, "LRU", 5);
        Iterator<IECSNode> it = nodes.iterator();
        IECSNode node1 = it.next();
        IECSNode node2 = it.next();

        //Connect to server4 because key b hashes to it
        KVStore kvClient = new KVStore("localhost", 50003);
        connectToKVServer(kvClient);
        assertTrue(ecsClient.start());

        putKVPair(kvClient, "b", "bb", StatusType.PUT_SUCCESS);
        kvClient.disconnect();

        //Make sure server7 is the last to be removed
        Collection<String> names = new ArrayList<String>();
        if(node1.getNodeName().equals("server7")) {
            names.add(node2.getNodeName());
            names.add(node1.getNodeName());
        } else {
            names.add(node1.getNodeName());
            names.add(node2.getNodeName());
        }
        //remove both servers
        assertTrue(ecsClient.removeNodes(names));

        //add new node.. should start up server7
        node1 = ecsClient.addNode("FIFO", 2);
        assertTrue(node1 != null);
        assertTrue(node1.getNodeName().equals("server7"));
        assertTrue(ecsClient.start());

        kvClient = new KVStore("localhost", node1.getNodePort());
        connectToKVServer(kvClient);
        //get should be successful
        getKVPair(kvClient, "b", "bb", StatusType.GET_SUCCESS);
    }
*/
    @Test
    public void testDetectServerCrash() {
        System.out.println("********** In 13 Test **************");
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
