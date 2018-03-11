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
import java.util.Iterator;
import java.nio.charset.StandardCharsets;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class PerformanceTesting extends TestCase {
    
    private ECSClient ecsClient;
    Collection<IECSNode> nodes;
    private String keyFilePath = System.getProperty("user.dir") + "/maildir/blair-l/sent_items";

    
	public void setUp() {
        System.out.println("Initializing tests");
        ecsClient = new ECSClient("testECS.config", "localhost");
        ecsClient.setLevel("INFO");
        try {
            Process p = Runtime.getRuntime().exec(new String[]{"csh","-c","rm -rf SERVER_5000*"});
        } catch (IOException e) {
            System.out.println("could not rm -rf: " + e); 
        }
	}

	public void tearDown() {
        System.out.println("Tearing down tests");
        ecsClient.shutdown();
        ecsClient.disconnect();
        try{
           TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException io){
            System.out.println("Delay caused an exception");
        }
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

    public String getValue(String key) {
        //Open file with than key name and append a string of the inputs value
        StringBuilder value = new StringBuilder();
        Path keyFile = Paths.get(this.keyFilePath + "/" + key + ".");
        if(Files.exists(keyFile)) {
            try{
                ArrayList<String> fileInput = new ArrayList<>(Files.readAllLines(keyFile,
                                                   StandardCharsets.UTF_8));
                for(String line :fileInput) {
                    value.append(line);
                }
            } catch (IOException ex) {
                System.out.println("File unable to be read");
            }
    
        }
        else {
            System.out.println("File doesn't exist");
        }
        return value.toString(); 
    } 


    public void  clientOneServer(int numClients) 
                   throws RuntimeException {

       nodes = ecsClient.addNodes(1, "LRU", 5);
       IECSNode node = nodes.iterator().next();
       assertTrue(ecsClient.start()); 
       String key = "4";
       String value = getValue(key);

       Collection<KVStore> clients = new ArrayList<KVStore>();

        for(int i = 0 ; i < numClients; i++) {    
           clients.add(new KVStore("localhost", node.getNodePort()));
        }

        for(KVStore kvClients: clients) {
            connectToKVServer(kvClients);
            putKVPair(kvClients, key, value, StatusType.PUT_SUCCESS);
            getKVPair(kvClients, key, value, StatusType.GET_SUCCESS);
        }
    
        for(KVStore kvClients: clients) {
            kvClients.disconnect();
        }   
       
        clients.clear();  
        Collection<String> names = new ArrayList<String>();
        names.add(node.getNodeName());
        assertTrue(ecsClient.removeNodes(names));
        names.clear();

 //-------------------------------------------------------------------//
        node = ecsClient.addNode("FIFO", 5);;
        assertTrue(ecsClient.start()); 
        
        for(int i = 0 ; i < numClients; i++) {    
           clients.add(new KVStore("localhost", node.getNodePort()));
        }

        for(KVStore kvClients: clients) {
            connectToKVServer(kvClients);
            putKVPair(kvClients, key, value, StatusType.PUT_SUCCESS);
            getKVPair(kvClients, key, value, StatusType.GET_SUCCESS);
        }
    
        for(KVStore kvClients: clients) {
            kvClients.disconnect();
        }   
       
        clients.clear();  
    
        names.add(node.getNodeName());
        assertTrue(ecsClient.removeNodes(names));
        names.clear();

 //-------------------------------------------------------------------//
        node = ecsClient.addNode("LFU", 5);;
        assertTrue(ecsClient.start()); 

        for(int i = 0 ; i < numClients; i++) {    
           clients.add(new KVStore("localhost", node.getNodePort()));
        }

        for(KVStore kvClients: clients) {
            connectToKVServer(kvClients);
            putKVPair(kvClients, key, value, StatusType.PUT_SUCCESS);
            getKVPair(kvClients, key, value, StatusType.GET_SUCCESS);
        }
    
        for(KVStore kvClients: clients) {
            kvClients.disconnect();
        }   
       
        clients.clear();  
    
        names.add(node.getNodeName());
        assertTrue(ecsClient.removeNodes(names));
        names.clear();

    } 



    public void  clientsFiveServersLRU(int numServers, int numClients) 
                   throws RuntimeException {

       nodes = ecsClient.addNodes(numServers, "LRU", 5);
//     IECSNode node = nodes.iterator().next();
       assertTrue(ecsClient.start()); 
       String key = "4";
       String value = getValue(key);

       Collection<String> names = new ArrayList<String>();
       Collection<KVStore> clients = new ArrayList<KVStore>();

        for(IECSNode node : nodes) {
            names.add(node.getNodeName());
           for(int i = 0 ; i < (numClients/numServers); i++) {    
              clients.add(new KVStore("localhost", node.getNodePort()));
            }
        }

        for(KVStore kvClients: clients) {
            connectToKVServer(kvClients);
            putKVPair(kvClients, key, value, StatusType.PUT_SUCCESS);
            getKVPair(kvClients, key, value, StatusType.GET_SUCCESS);
        }
    
        for(KVStore kvClients: clients) {
            kvClients.disconnect();
        }   
       
        clients.clear();  
        assertTrue(ecsClient.removeNodes(names));
        names.clear();
    }


    public void  clientsFiveServersFIFO(int numServers, int numClients) 
                   throws RuntimeException {

       nodes = ecsClient.addNodes(numServers, "FIFO", 5);
       assertTrue(ecsClient.start()); 
       String key = "4";
       String value = getValue(key);
       assertTrue(ecsClient.start()); 

       Collection<String> names = new ArrayList<String>();
       Collection<KVStore> clients = new ArrayList<KVStore>();

        for(IECSNode node : nodes) {
            names.add(node.getNodeName());
           for(int i = 0 ; i < (numClients/numServers); i++) {    
              clients.add(new KVStore("localhost", node.getNodePort()));
            }
        }

        for(KVStore kvClients: clients) {
            connectToKVServer(kvClients);
            putKVPair(kvClients, key, value, StatusType.PUT_SUCCESS);
            getKVPair(kvClients, key, value, StatusType.GET_SUCCESS);
        }
    
        for(KVStore kvClients: clients) {
            kvClients.disconnect();
        }   
       
        clients.clear();  
        assertTrue(ecsClient.removeNodes(names));
        names.clear();

    }

    public void  clientsFiveServersLFU(int numServers, int numClients) 
                   throws RuntimeException {

       nodes = ecsClient.addNodes(numServers, "LFU", 5);
       assertTrue(ecsClient.start()); 
       String key = "4";
       String value = getValue(key);
       assertTrue(ecsClient.start()); 

       Collection<String> names = new ArrayList<String>();
       Collection<KVStore> clients = new ArrayList<KVStore>();

        for(IECSNode node : nodes) {
            names.add(node.getNodeName());
           for(int i = 0 ; i < (numClients/numServers); i++) {    
              clients.add(new KVStore("localhost", node.getNodePort()));
            }
        }

        for(KVStore kvClients: clients) {
            connectToKVServer(kvClients);
            putKVPair(kvClients, key, value, StatusType.PUT_SUCCESS);
            getKVPair(kvClients, key, value, StatusType.GET_SUCCESS);
        }
    
        for(KVStore kvClients: clients) {
            kvClients.disconnect();
        }   
       
        clients.clear();  
        assertTrue(ecsClient.removeNodes(names));
        names.clear();

    } 


    // ------------------------------ tests start here -----------------------------------------//
    

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

/*    @Test
    public void testOneClientOneServer() {
       try{
           clientOneServer(1); 
           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 


    @Test
    public void testFiveClientOneServer() {
       try{
           clientOneServer(5); 
           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 
    @Test
    public void testTwentyClientOneServer() {
       try{
           clientOneServer(20); 
           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 

    @Test
    public void testFiftyClientOneServer() {
       try{
           clientOneServer(50); 
           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 


    @Test
    public void testHundredClientOneServer() {
       try{
           clientOneServer(100); 
           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 


    @Test
    public void testFiveClientsFiveServer() {
       try{
           clientsFiveServersLRU(5, 5);
           clientsFiveServersFIFO(5, 5);
           clientsFiveServersLFU(5, 5);

           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 

    @Test
    public void testTwentyClientsFiveServer() {
       try{
           clientsFiveServersLRU(5, 20);
           clientsFiveServersFIFO(5, 20);
           clientsFiveServersLFU(5, 20);

           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 
    @Test
    public void testFiftyClientsFiveServer() {
       try{
           clientsFiveServersLRU(5, 50);
           clientsFiveServersFIFO(5, 50);
           clientsFiveServersLFU(5, 50);

           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 

    @Test
    public void testHundredClientFiveServer() {
       try{
           clientsFiveServersLRU(5, 100);
           clientsFiveServersFIFO(5, 100);
           clientsFiveServersLFU(5, 100);

           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 
    
    @Test
    public void testTwentyClientsTwentyServer() {
       try{
           clientsFiveServersLRU(20, 20);
           clientsFiveServersFIFO(20, 20);
           clientsFiveServersLFU(20, 20);

           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    }
 
    @Test
    public void testFiftyClientsTwentyServer() {
       try{
           clientsFiveServersLRU(20, 50);
           clientsFiveServersFIFO(20, 50);
           clientsFiveServersLFU(20, 50);

           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 

 
    @Test
    public void testHundredClientsTwentyServer() {
       try{
           clientsFiveServersLRU(20, 100);
           clientsFiveServersFIFO(20, 100);
           clientsFiveServersLFU(20, 100);

           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    }
 
    @Test
    public void testFiftyClientsFiftyServer() {
       try{
           clientsFiveServersLRU(50, 50);
           clientsFiveServersFIFO(50, 50);
           clientsFiveServersLFU(50, 50);

           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 

    @Test
    public void testHundredClientsFiftyServer() {
       try{
           clientsFiveServersLRU(50, 100);
           clientsFiveServersFIFO(50, 100);
           clientsFiveServersLFU(50, 100);

           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 
    @Test
    public void testHundredClientHundredServer() {
       try{
           clientsFiveServersLRU(100, 100);
           clientsFiveServersFIFO(100, 100);
           clientsFiveServersLFU(100, 100);

           } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 
    */
}
