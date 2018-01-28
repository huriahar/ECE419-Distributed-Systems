package testing;

import org.junit.Test;

import junit.framework.TestCase;
import cache.KVCacheLFU;
import cache.KVCacheLRU;
import cache.KVCacheFIFO;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import app_kvServer.KVServer;
import java.io.IOException;

public class CacheTests extends TestCase {

	private KVCacheFIFO fifo_cache;
	private KVCacheLRU lru_cache;
	private KVCacheLFU lfu_cache;
	private KVStore kvClient;
    private KVServer kvServer;
	
	public void setUp() {
        fifo_cache = new KVCacheFIFO(3);
        lru_cache = new KVCacheLRU(3);
        lfu_cache = new KVCacheLFU(3);

	}

	public void tearDown() {
	}

    // The following tests test the replacement policies of each type of cache
    // Test insertion, deletion, eviction and replacement
    @Test
    public void  testFIFOreplacement() { 
        fifo_cache.insert("a", "1");
        fifo_cache.insert("b", "2");
        fifo_cache.insert("c", "3");
        fifo_cache.insert("a", "1111");

        assertTrue(fifo_cache.getValue("a") == "1111");

        fifo_cache.insert("d", "4");

        assertTrue(fifo_cache.hasKey("b"));
        assertTrue(fifo_cache.hasKey("c"));
        assertTrue(fifo_cache.hasKey("d"));
        assertFalse(fifo_cache.hasKey("a"));

        fifo_cache.delete("b");
        fifo_cache.insert("e", "5");
        fifo_cache.insert("f", "6");
        assertFalse(fifo_cache.hasKey("b"));
        assertFalse(fifo_cache.hasKey("c"));
    }

    @Test
    public void  testLRUreplacement() { 
        lru_cache.insert("a", "1");
        lru_cache.insert("b", "2");
        lru_cache.insert("c", "3");
        lru_cache.insert("a", "1111");

        assertTrue(lru_cache.getValue("a") == "1111");

        lru_cache.insert("d", "4");

        assertTrue(lru_cache.hasKey("a"));
        assertTrue(lru_cache.hasKey("c"));
        assertTrue(lru_cache.hasKey("d"));
        assertFalse(lru_cache.hasKey("b"));
        
        lru_cache.delete("c");
        lru_cache.insert("e", "5");
        lru_cache.insert("f", "6");
        assertFalse(fifo_cache.hasKey("c"));
        assertFalse(fifo_cache.hasKey("d"));
    }

    @Test
    public void  testLFUreplacement() { 
        lfu_cache.insert("a", "1");
        lfu_cache.insert("b", "2");
        lfu_cache.insert("b", "22");
        lfu_cache.insert("c", "3");
        lfu_cache.insert("a", "1111");

        assertTrue(lfu_cache.getValue("a") == "1111");

        lfu_cache.insert("d", "4");

        assertTrue(lfu_cache.hasKey("a"));
        assertTrue(lfu_cache.hasKey("b"));
        assertTrue(lfu_cache.hasKey("d"));
        assertFalse(lfu_cache.hasKey("c"));

        lfu_cache.insert("a", "1111");
        // f(a) = 4
        // f(b) = 2
        // f(d) = 1
        lfu_cache.delete("d");
        lfu_cache.insert("e", "5");
        lfu_cache.insert("f", "6");
        // expect b to be replaced
        assertFalse(lfu_cache.hasKey("e"));
    }

    // The following tests ensure that after retrieving a value from disk
    // the cache is updates as expected
    @Test
    public void  testEvictRetrieve() {
        // Server needed to test disk consistency with cache
        // Initialize server
        kvServer = new KVServer(1234, 3, "FIFO");
        kvServer.start();

        // Initialize client
		kvClient = new KVStore("localhost",1234);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
        Exception ex = null;

        // Body of the test
        try {
            kvClient.put("a", "1");
            kvClient.put("b", "2");
            kvClient.put("c", "3");
            kvClient.put("d", "4");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(kvServer.inCache("d"));
        assertFalse(kvServer.inCache("a"));

		KVMessage response = null;
		ex = null;

		try {
			response = kvClient.get("a");
		} catch (Exception e) {
			ex = e;
		}

        assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS && response.getValue().equals("1"));

		kvClient.disconnect();
        kvServer.close();
    } 
}

