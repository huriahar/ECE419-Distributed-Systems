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
import java.lang.Process;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

import java.util.concurrent.TimeUnit;

public class PerformanceTesting extends TestCase {
    
    private ECSClient ecsClient;
    Collection<IECSNode> nodes;
    long startTime;
    private String keyFilePath = System.getProperty("user.dir") + "/maildir/blair-l/sent_items";
    private static final Path enronData = Paths.get("enron.txt");
    
	public void setUp() {
        System.out.println("Initializing tests");
        ecsClient = new ECSClient("testPerformance.config", "localhost");
        ecsClient.setLevel("INFO");
        clearStorage();
        this.startTime = System.currentTimeMillis();
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
        System.out.println("END TIMING FOR THIS TEST " + (System.currentTimeMillis() - this.startTime));
	}

    public void clearStorage() {
        try {
            Process p = Runtime.getRuntime().exec(new String[]{"csh","-c","rm -rf SERVER_500*"});
            System.out.println("Cleared Storage"); 
        } catch (IOException e) {
            System.out.println("could not rm -rf: " + e); 
        }
    }    

    // ------------------------------ helper functions for tests --------------------------------//
    public void connectToKVServer(KVStore kvClient) {
        Exception ex = null;
        try {
            kvClient.connect();
        } catch (Exception e) {
            System.out.println("ERROR when connecting to client: " + e);
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
            System.out.println("ERROR when getting key: " + e);
            ex = e;
        }
        assertTrue(response != null && response.getStatus().equals(expectedStatus)
        && response.getValue().equals(value));
    }

    public void putKVPair(KVStore kvClient, String key, String value, StatusType expectedStatus, StatusType expectedStatus2) {
        KVMessage response = null;
        Exception ex = null;
        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            System.out.println("ERROR when putting key: " + e);
            ex = e;
        }
        assertTrue(response != null && (response.getStatus().equals(expectedStatus) || response.getStatus().equals(expectedStatus)));
    }

    public void putKVPair(KVStore kvClient, String key, String value, StatusType expectedStatus) {
        KVMessage response = null;
        Exception ex = null;
        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            System.out.println("ERROR when putting key: " + e);
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

/*
    public void  clientOneServer(int numClients, String strategy) 
                   throws RuntimeException {

       nodes = ecsClient.addNodes(1, strategy, 3);
       IECSNode node = nodes.iterator().next();
       assertTrue(ecsClient.start()); 

       Collection<KVStore> clients = new ArrayList<KVStore>();

        for(int i = 0 ; i < numClients; i++) {    
           clients.add(new KVStore("localhost", node.getNodePort()));
        }

        for(KVStore kvClients: clients) {
            clearStorage();
            connectToKVServer(kvClients);
            putKVPair(kvClients, "a", "1",StatusType.PUT_SUCCESS);
            putKVPair(kvClients, "b", "1",StatusType.PUT_SUCCESS);
            putKVPair(kvClients, "c", "1",StatusType.PUT_SUCCESS);
            getKVPair(kvClients, "a", "1",StatusType.GET_SUCCESS);
            getKVPair(kvClients, "a", "1",StatusType.GET_SUCCESS);
            getKVPair(kvClients, "a", "1",StatusType.GET_SUCCESS);
            getKVPair(kvClients, "b", "1",StatusType.GET_SUCCESS);
            getKVPair(kvClients, "b", "1",StatusType.GET_SUCCESS);
            putKVPair(kvClients, "d", "1",StatusType.PUT_SUCCESS);
            putKVPair(kvClients, "e", "1",StatusType.PUT_SUCCESS);
            getKVPair(kvClients, "a", "1",StatusType.GET_SUCCESS);
            getKVPair(kvClients, "b", "1",StatusType.GET_SUCCESS);
        }
        
    
        for(KVStore kvClients: clients) {
            kvClients.disconnect();
        }   
       
        clients.clear();  
        Collection<String> names = new ArrayList<String>();
        names.add(node.getNodeName());
        assertTrue(ecsClient.removeNodes(names));
        names.clear();
   }
*/
    public void testBasicFunctions(int numServers, int cacheSize, String strategy) {
        System.out.println("Running test for numServers " + numServers + " cacheSize " + cacheSize + " strategy " + strategy);
        clearStorage();
        long startTime = System.currentTimeMillis();
        nodes = ecsClient.addNodes(numServers, strategy, cacheSize);
        IECSNode node = nodes.iterator().next();
        assertTrue(ecsClient.start()); 
        
        System.out.println("Connecting test for numServers " + numServers + " cacheSize " + cacheSize + " strategy " + strategy);
        KVStore kvClient = new KVStore("localhost", node.getNodePort());
        connectToKVServer(kvClient);
        return;
        /*
        ArrayList<String> lines = new ArrayList<>();
        try {
            lines = new ArrayList(Files.readAllLines(this.enronData, StandardCharsets.UTF_8));
        } catch (IOException e ) {
            System.out.println(e);
            return;
        }
        for(String line : lines) {
            String[] kvp = line.split("\\|");
            putKVPair(kvClient, kvp[0], kvp[1], StatusType.PUT_SUCCESS, StatusType.PUT_UPDATE);
        }
        for(String line : lines) {
            String[] kvp = line.split("\\|");
            getKVPair(kvClient, kvp[0], kvp[1], StatusType.GET_SUCCESS);
        }
        long endTime = System.currentTimeMillis() - startTime;
        System.out.println("Duration for numServers " + numServers + " cacheSize " + cacheSize + " strategy " + strategy);
        */
    }
/*
    public void  clientServer(int numClients, int numServers, String strategy) 
                   throws RuntimeException {

       nodes = ecsClient.addNodes(numServers, strategy, 3);
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
            clearStorage();
            putKVPair(kvClients, "a", "1",StatusType.PUT_SUCCESS, StatusType.PUT_UPDATE);
            putKVPair(kvClients, "b", "1",StatusType.PUT_SUCCESS, StatusType.PUT_UPDATE);
            putKVPair(kvClients, "c", "1",StatusType.PUT_SUCCESS, StatusType.PUT_UPDATE);
            getKVPair(kvClients, "a", "1",StatusType.GET_SUCCESS);
            getKVPair(kvClients, "a", "1",StatusType.GET_SUCCESS);
            getKVPair(kvClients, "a", "1",StatusType.GET_SUCCESS);
            getKVPair(kvClients, "b", "1",StatusType.GET_SUCCESS);
            getKVPair(kvClients, "b", "1",StatusType.GET_SUCCESS);
            putKVPair(kvClients, "d", "1",StatusType.PUT_SUCCESS, StatusType.PUT_UPDATE);
            putKVPair(kvClients, "e", "1",StatusType.PUT_SUCCESS, StatusType.PUT_UPDATE);
            getKVPair(kvClients, "a", "1",StatusType.GET_SUCCESS);
            getKVPair(kvClients, "b", "1",StatusType.GET_SUCCESS);
        }
    
        for(KVStore kvClients: clients) {
            kvClients.disconnect();
        }   
       
        clients.clear();  
        assertTrue(ecsClient.removeNodes(names));
        names.clear();
    }
*/

    public long printDelta() {
        long delta = (System.currentTimeMillis() - this.startTime);
        System.out.println("END TIMING FOR THIS TEST " + delta);
        this.startTime = System.currentTimeMillis();
        return delta;
    }



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
    // ------------------------------ tests start here -----------------------------------------//
   
    @Test
    public void testOneClientOneServer() {
       try{
           clientOneServer(1, "FIFO"); 
           long t1 = printDelta(); 
            clearStorage();

           clientOneServer(1, "LRU"); 
           long t2 = printDelta(); 
            clearStorage();

           clientOneServer(1, "LFU"); 
           long t3 = printDelta(); 
            clearStorage();

            System.out.println("FIFO t = " + t1 + " LRU t = " + t2 + " LFU t = " + t3);
        } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 
*/
    @Test
    public void testParams() {
            int [] numServers = {1};
            int [] cacheSize = {1};
            String [] cacheStrategy = {"FIFO", "LRU", "LFU"};
            for(int i : numServers) {
                for(int j : cacheSize) {
                    for(String k : cacheStrategy) {
                        testBasicFunctions(i, j, k);
                    }
                }                
            }

    }
/*
    @Test
    public void testClientServer() {
       try{
            int [] num = {5};
            for(int i = 0; i < num.length; i++) {
                for(int j = i; j < num.length; j++) {
                    System.out.println("----------------------numClients: " + num[i] + " numServer: " + num[j] + "-----------------------------"); 
                   clientServer(num[i], num[j], "FIFO"); 
                   long t1 = printDelta(); 

                   clientServer(num[i], num[j], "LRU"); 
                   long t2 = printDelta(); 

                   clientServer(num[i], num[j], "LFU"); 
                   long t3 = printDelta(); 

                    System.out.println("FIFO t = " + t1 + " LRU t = " + t2 + " LFU t = " + t3);
                }                

            }

        } catch (Exception io) {
            throw new RuntimeException(io);
        }
    } 
*/ 
}
