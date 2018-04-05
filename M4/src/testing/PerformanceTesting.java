package testing;

import org.junit.Test;

import junit.framework.TestCase;

import client.KVStore;
import common.KVConstants;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import ecs.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import app_kvECS.*;
import java.util.Collection;
import java.lang.Process;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PerformanceTesting extends TestCase {

    private ECSClient ecsClient;
    ArrayList<IECSNode> nodes;
    ArrayList<KVStore> clients;
    long startTime;
    private String keyFilePath = System.getProperty("user.dir") + "/maildir/blair-l/inbox";
    private static final Path enronData = Paths.get("enron.txt");

    public void setUp() {
        System.out.println("Initializing tests");
        ecsClient = new ECSClient("testPerformance.config", "localhost");
        ecsClient.setLevel("INFO");
        nodes = new ArrayList<IECSNode>();
        clients = new ArrayList<KVStore>();
        clearStorage();
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
            Runtime.getRuntime().exec(new String[]{"csh","-c","rm -rf SERVER_8* outserver* lastRemoved.txt"});
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
        assertTrue(response != null);
        assertTrue(response.getStatus().equals(expectedStatus));
        //assertTrue(response.getValue().equals(value));
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

    public void launchServers(int numServers, int cacheSize, String strategy) {
        System.out.println("Running test for numServers " + numServers + " cacheSize " + cacheSize + " strategy " + strategy);
        clearStorage();
        nodes.clear();
        nodes = (ArrayList<IECSNode>) ecsClient.addNodes(numServers, strategy, cacheSize);
        assertTrue(ecsClient.start()); 
        return;
    }

    public void launchClients(int numClients) {
        clients.clear();
        System.out.println("Starting with numClients: " + numClients + " and numServers: " + nodes.size());
        int numServers = nodes.size();
        if (numClients > numServers) {
            int clientsPerServer = numClients/numServers;
            for (int i = 0; i < numServers; ++i) {
                for (int j = 0; j < clientsPerServer; ++j) {
                    clients.add(new KVStore("localhost", nodes.get(i).getNodePort()));
                }
            }
            numClients -= clientsPerServer*numServers;
        }
        assert(numClients <= numServers);
        assert(nodes.size() > 0);
        for (int i = 0; i < numClients; ++i) {
            IECSNode node = nodes.get(i);
            assert(node != null);
            clients.add(new KVStore("localhost", node.getNodePort()));
        }
        System.out.println("Added the following clients:");
        for (int i = 0; i < clients.size(); ++i) {
            connectToKVServer(clients.get(i));
            System.out.println("Client " + i + " connected to port " + clients.get(i).getServerPort());
        }
    }
    
    public String getValueFromFile (File filePath) {
        ArrayList<String> lines = null;
        try {
            lines = new ArrayList<String>(Files.readAllLines(filePath.toPath(), StandardCharsets.UTF_8));
        }
        catch (IOException ex) {
            System.out.println(ex);
            return "";
        }
        return String.join(KVConstants.NEWLINE_DELIM, lines);
    }
    
    public void populateStorageService() {
        File folder = new File(keyFilePath);
        File[] listOfFiles = folder.listFiles();
        String value = "";
        int clientIdx = 0;
        for (int i = 0; i < listOfFiles.length; ++i) {
            if (listOfFiles[i].isFile()) {
                value = getValueFromFile(listOfFiles[i]);
                clientIdx = i % clients.size();
                putKVPair(clients.get(clientIdx), listOfFiles[i].getName(), value.trim(), StatusType.PUT_SUCCESS, StatusType.PUT_UPDATE);
            }
        }
    }
    
    public void retrieveKeys() {
        File folder = new File(keyFilePath);
        File[] listOfFiles = folder.listFiles();
        int clientIdx = 0;
        String value;
        for (int i = 0; i < listOfFiles.length; ++i) {
            if (listOfFiles[i].isFile()) {
                clientIdx = i % clients.size();
                value = getValueFromFile(listOfFiles[i]);
                getKVPair(clients.get(clientIdx), listOfFiles[i].getName(), value.trim(), StatusType.GET_SUCCESS);
            }
        }
    }

    public void printDelta() {
        long delta = (System.currentTimeMillis() - this.startTime);
        System.out.println("END TIMING FOR THIS TEST " + delta);
    }

    // ------------------------------ tests start here -----------------------------------------//

    @Test
    public void testParams() {
        int numServers = 20;        // 1, 5, 20
        int cacheSize = 5;
        String [] cacheStrategy = {"FIFO", "LRU", "LFU"};
        String strategy = cacheStrategy[0];
        int numClients = 1;         // 1, 5, 20
        launchServers(numServers, cacheSize, strategy);
        launchClients(numClients);
        this.startTime = System.currentTimeMillis();
        populateStorageService();
        retrieveKeys();
        System.out.println("Done with numServers: " + numServers + " strategy: "+ strategy + " size: " + cacheSize + " and numClients " + numClients);
        printDelta();
    }
}
