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
import java.util.Collections;
import java.math.BigInteger;
import common.*;
import java.lang.Process;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;


public class M4Test extends TestCase {
    
    private ECSClient ecsClient;
    Collection<IECSNode> nodes;

	public void setUp() {
        ecsClient = new ECSClient("testECS.config", "localhost");
        ecsClient.setLevel("INFO");
        try {
            Process p = Runtime.getRuntime().exec(new String[]{"csh","-c","rm -rf SERVER_5000* SERVER_6000*"});
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

    public boolean checkIfKeyInFile(Path filePath, boolean expectToFind, String value) {
        Exception ex = null;
        boolean success = false;
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
                for(String line: lines) {
                    if(line.equals(value))
                        success = true;
                }
            }
        } else {
            assertTrue(ex != null);
        }
        return success;
    }
    // ------------------------------ tests start here -----------------------------------------//


    @Test
    public void testDistributedEvenNumberKeys() {
        System.out.println("********** In 1 Test **************");
        nodes = ecsClient.addNodes(2, "LRU", 5);
        Iterator<IECSNode> it = nodes.iterator();
        assertTrue(ecsClient.start());
        //Connect client to server and put KV pair
        KVStore kvClient = new KVStore("localhost", 50006);
        connectToKVServer(kvClient);
        putKVPair(kvClient, "bb", "bb", StatusType.PUT_SUCCESS);
        putKVPair(kvClient, "cc", "bb", StatusType.PUT_SUCCESS);
        putKVPair(kvClient, "dd", "bb", StatusType.PUT_SUCCESS);
        putKVPair(kvClient, "aa", "bb", StatusType.PUT_SUCCESS);
        kvClient.disconnect();

        boolean success = ecsClient.balanceServerLoad();

        if(success) {
            //Check the content of the files to double check if the keys are hashed evenly 
            String fileName = "SERVER_50006"; 
            assertTrue(checkIfKeyInFile(Paths.get(fileName), true, "cc|bb"));

            fileName = "SERVER_50006"; 
            assertTrue(checkIfKeyInFile(Paths.get(fileName), true, "dd|bb"));
            
            fileName = "SERVER_50003"; 
            assertTrue(checkIfKeyInFile(Paths.get(fileName), true, "aa|bb"));
     
            fileName = "SERVER_50003"; 
            assertTrue(checkIfKeyInFile(Paths.get(fileName), true, "bb|bb"));
        }

    }

    @Test
    public void testDistributedOddNumberKeys() {
        System.out.println("********** In 2 Test **************");
        nodes = ecsClient.addNodes(2, "LRU", 5);
        Iterator<IECSNode> it = nodes.iterator();
        assertTrue(ecsClient.start());
        //Connect client to server and put KV pair
        KVStore kvClient = new KVStore("localhost", 50006);
        connectToKVServer(kvClient);
        putKVPair(kvClient, "bb", "bb", StatusType.PUT_SUCCESS);
        putKVPair(kvClient, "cc", "bb", StatusType.PUT_SUCCESS);
        putKVPair(kvClient, "dd", "bb", StatusType.PUT_SUCCESS);
        kvClient.disconnect();

        boolean success = ecsClient.balanceServerLoad();

        if(success) {
            //Check the content of the files to double check if the keys are hashed evenly 
            String fileName = "SERVER_50006"; 
            assertTrue(checkIfKeyInFile(Paths.get(fileName), true, "cc|bb"));

            fileName = "SERVER_50003"; 
            assertTrue(checkIfKeyInFile(Paths.get(fileName), true, "dd|bb"));
     
            fileName = "SERVER_50003"; 
            assertTrue(checkIfKeyInFile(Paths.get(fileName), true, "bb|bb"));
        }

    }
}
