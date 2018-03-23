package testing;

import org.junit.Test;

import junit.framework.TestCase;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import java.io.IOException;
import ecs.*;
import app_kvECS.*;

public class MultipleClients extends TestCase {

    private ECSClient ecsClient;
    IECSNode newNode;
	private KVStore kvClient1;
	private KVStore kvClient2;
	private KVStore kvClient3;
	
	public void setUp() {
        ecsClient = new ECSClient("testECS.config", "localhost");
        ecsClient.setLevel("INFO");
        try {
            Process p = Runtime.getRuntime().exec(new String[]{"csh","-c","rm -rf SERVER_5000*"});
        } catch (IOException e) {
            System.out.println("could not rm -rf: " + e); 
        }
        newNode = ecsClient.addNode("LRU", 2);
        ecsClient.start();
        // Initialize client
		kvClient1 = new KVStore("localhost", newNode.getNodePort());
		kvClient2 = new KVStore("localhost", newNode.getNodePort());
		kvClient3 = new KVStore("localhost", newNode.getNodePort());
		try {
			kvClient1.connect();
			kvClient2.connect();
			kvClient3.connect();
		} catch (Exception e) {
		}
        Exception ex = null;
	}

	public void tearDown() {
		kvClient1.disconnect();
		kvClient2.disconnect();
		kvClient3.disconnect();
        ecsClient.shutdown();
	}

    @Test
    public void  testDataAccess() {
		KVMessage response = null;
		Exception ex = null;
        // Body of the test
        try {
            kvClient1.put("a", "1");
            response = kvClient2.get("a");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS && response.getValue().equals("1"));
    } 

    @Test
    public void  testDataUpdate() {
		KVMessage response = null;
		Exception ex = null;
        // Body of the test
        try {
            kvClient1.put("a", "1");
            kvClient1.put("b", "2");
            kvClient2.put("a", "1111");
            response = kvClient3.get("a");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS && response.getValue().equals("1111"));
    } 

    @Test
    public void  testDataDelete() {
		KVMessage response = null;
		Exception ex = null;
        // Body of the test
        try {
            kvClient1.put("a", "1");
            kvClient1.put("b", "2");
            kvClient2.put("a", "");
            response = kvClient3.get("a");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
    }
}

