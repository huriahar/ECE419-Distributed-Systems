package testing;

import org.junit.Test;

import junit.framework.TestCase;

import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

public class AdditionalTest extends TestCase {
    
    // TODO add your test cases, at least 10

    private KVStore kvClient;
    
    public void setUp() {
        kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
        }
    }

    public void tearDown() {
        kvClient.disconnect();
    }
    
    @Test
    public void testInvalidKeySpace() {
        String key = "A B";
        String value = "C D";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
    }

    @Test
    public void testInvalidKeyDelim() {
        String key = "A|B";
        String value = "C D";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
    }

    @Test
    public void testValidValueDelim() {
        String key = "AB";
        String value = "C|D";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS 
            && response.getValue().equals("C|D"));
    }

    @Test
    public void testNonExistentKey() {
        String key = "AB";
        String value = "CD";
        String invalidKey = "CD";
        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            response = kvClient.get(invalidKey);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
    }    
}
