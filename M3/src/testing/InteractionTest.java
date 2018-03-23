package testing;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import ecs.*;
import app_kvECS.*;
import java.io.IOException;


public class InteractionTest extends TestCase {

    private ECSClient ecsClient;
	private KVStore kvClient;
    IECSNode newNode;
	
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

		kvClient = new KVStore("localhost", newNode.getNodePort());
		try {
			kvClient.connect();
		} catch (Exception e) {
            System.out.println("ERROR when connecting to client");
		}
	}

	public void tearDown() {
		kvClient.disconnect();
        ecsClient.shutdown();
	}
	
	
	@Test
	public void testPut() {
		String key = "foo2";
		String value = "bar2";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}
	
	@Test
	public void testPutDisconnected() {
		kvClient.disconnect();
		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}
	
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}
	
	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}
}
