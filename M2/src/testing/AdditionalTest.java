package testing;

import org.junit.Test;

import junit.framework.TestCase;

import client.KVStore;
import app_kvServer.KVServer;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import ecs.*;
import java.util.Map;
import java.util.ArrayList;
import app_kvECS.*;
import java.util.HashMap;
import java.util.Collection;
import java.math.BigInteger;
import common.*;
import java.lang.Process;
import java.io.IOException;

public class AdditionalTest extends TestCase {
    
    private ECSClient ecsClient;
    Collection<IECSNode> nodes;
    
	public void setUp() {
        ecsClient = new ECSClient("testECS.config");
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
/*
    @Test
    public void  testInitialization() {
       try{
            System.out.println("********** In First Test **************");
            
            nodes = ecsClient.addNodes(1, "LRU", 5);
            assertTrue(ecsClient.start()); 
            //assertTrue(ecsClient.shutdown());
            assertTrue(ecsClient.stop());
        } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 

    @Test
    public void testHashing() {
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
    public void testKeyInput() {
        System.out.println("********** In 3 Test **************");
        nodes = ecsClient.addNodes(1, "LRU", 5);
        IECSNode init = new ECSNode();
        IECSNode newNode = new ECSNode();
        String key = "a"; 
        String value = "1";
        KVMessage response = null;
        Exception ex = null;
        Collection<String> name = new ArrayList<String>();;
 
        for(IECSNode node: nodes) {
            init = node;
            name.add(node.getNodeName());
        }
        ecsClient.start();
        KVStore kvClient = new KVStore("localhost", init.getNodePort());
        try {
            kvClient.connect();
        } catch (Exception e) {
        } 
        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
            System.out.println("ERROR when putting key?");
        }
        System.out.println("response is: " + response.getStatus()); 
        assertTrue(ex == null && response.getStatus().equals(StatusType.PUT_SUCCESS));
        kvClient.disconnect();
        
        try {
            newNode = ecsClient.addNode("LRU", 2);
            assertTrue(newNode != null);
            ecsClient.addToLaunchedNodes(newNode);
            assertTrue(ecsClient.removeNodes(name));
            System.out.println("before starting.....");
            assertTrue(ecsClient.start());
            if(newNode != null) {
                kvClient = new KVStore("localhost", newNode.getNodePort());
                try {
                    kvClient.connect();
                } catch (Exception e) {
                    System.out.println("Connection unable");   
                } 
            }
        } catch (Exception e) {
            System.out.println("ERROR when adding or removing node?");
        }

        
        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            System.out.println("ERROR when getting key");
        }
        System.out.println("response is: " + response.getStatus()); 
        assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS 
        && response.getValue().equals("1"));
    } 

    @Test
    public void testConnectToWrongServer() {
        System.out.println("********** In 4 Test **************");
        nodes = ecsClient.addNodes(2, "LRU", 5);
        //key hashes to server4 when only server7 and server4 available
        String key = "b"; 
        String value = "1";
        KVMessage response = null;
        Exception ex = null;
        
        ecsClient.start();
        //connect to server7
        KVStore kvClient = new KVStore("localhost", 50006);
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null);
        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            System.out.println("ERROR when putting key");
            ex = e;
        }
        assertTrue(response != null);
        StatusType status = response.getStatus();

        assertTrue(status.equals(StatusType.PUT_SUCCESS));
        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            System.out.println("ERROR when getting key");
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS 
        && response.getValue().equals("1"));
        kvClient.disconnect();
    } 

    @Test
    public void testServerStopped() {
        System.out.println("********** In 5 Test **************");
        nodes = ecsClient.addNodes(1, "FIFO", 5);
        int port = nodes.iterator().next().getNodePort();
        //key hashes to server4 when only server7 and server4 available
        String key = "b"; 
        String value = "1";
        KVMessage response = null;
        Exception ex = null;
        //connect to server7
        KVStore kvClient = new KVStore("localhost", port);
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null);
        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            System.out.println("ERROR when putting key");
            ex = e;
        }
        assertTrue(response != null);
        StatusType status = response.getStatus();
        assertTrue(status.equals(StatusType.SERVER_STOPPED));
    }
*/

    @Test
    public void testInitTooManyServers() {
        System.out.println("********** In 6 Test **************");
        nodes = ecsClient.addNodes(25, "FIFO", 5);
        assertTrue(nodes == null);
    }
}
