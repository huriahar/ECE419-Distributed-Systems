package testing;

import org.junit.Test;

import junit.framework.TestCase;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import java.io.IOException;

public class MultipleClients extends TestCase {

	private KVStore kvClient1;
	private KVStore kvClient2;
	private KVStore kvClient3;
	
	public void setUp() {
        // Initialize client
		kvClient1 = new KVStore("localhost", 50000);
		kvClient2 = new KVStore("localhost", 50000);
		kvClient3 = new KVStore("localhost", 50000);
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

