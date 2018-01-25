package cache;

import java.util.LinkedList;
import java.util.HashMap;
import java.util.Map;

import app_kvServer.IKVServer;

public class KVCacheFIFO extends KVCache {

    // Using a linked list implementation of a queue
    // Maintians FIFO order but allows random deletion
    private LinkedList<String> fifo = new LinkedList<String>();
    private Map<String, String> kvp_map = new HashMap();

    public KVCacheFIFO(int cacheSize) {
        super(cacheSize, "FIFO");
    }

    @Override
    public void insert(String key, String value){
        if(value.equals("")) return;

        // check if it already exists
        if (kvp_map.containsKey(key)) {
            //update the value
            kvp_map.put(key, value);
            return;
        }
            
        // if it doesn't exist, cache it
        // if the cache is full, evict according to replacement policy
        if(fifo.size() == this.getCacheSize()) {
            //FIFO replacement
            fifo.removeLast();
        }
        fifo.add(key);
        kvp_map.put(key, value);
    }

    @Override
    public void delete(String key){
        if( kvp_map.containsKey(key)) {
            kvp_map.remove(key);

            if( fifo.contains(key))
                fifo.remove(key);
        }
    }

    @Override
    public boolean hasKey(String key){
        return kvp_map.containsKey(key);
    }

    @Override
    public String getValue(String key){
        if(hasKey(key))
            return kvp_map.get(key);
        return "";
    }

    @Override
    public void clearCache(){
        fifo.clear(); 
        kvp_map.clear();
    }

}
