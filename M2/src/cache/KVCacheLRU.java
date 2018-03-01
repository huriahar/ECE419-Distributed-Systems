package cache;

import java.util.LinkedList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.Set;

public class KVCacheLRU extends KVCache {
    // Using a linked list implementation of a queue
    // Maintians FIFO order but allows random deletion
    private LinkedList<String> fifo = new LinkedList<String>();
    private Map<String, String> kvp_map = new HashMap();


    public KVCacheLRU(int cacheSize) {
        super(cacheSize, "LRU");
    }

    @Override
    public synchronized void insert(String key, String value){
        if(value.equals("")) return;

        // check if it already exists
        if (kvp_map.containsKey(key)) {
            //update the key's position in the FIFO
            fifo.remove(key);
        }
            
        // if it doesn't exist, cache it
        // if the cache is full, evict according to replacement policy
        if(fifo.size() == this.getCacheSize()) {
            //LRU replacement
            String first = fifo.removeFirst();
            kvp_map.remove(first);
        }
        fifo.add(key);
        kvp_map.put(key, value);
    }

    @Override
    public synchronized void delete(String key) {
        if( kvp_map.containsKey(key)) {
            kvp_map.remove(key);

            if( fifo.contains(key))
                fifo.remove(key);
        }
    }

    @Override
    public synchronized boolean hasKey(String key){
        return kvp_map.containsKey(key);
    }

    @Override
    public synchronized String getValue(String key){
        if(hasKey(key))
            return kvp_map.get(key);
        return "";
    }

    @Override
    public synchronized void clearCache(){
        fifo.clear(); 
        kvp_map.clear();
    }

    @Override
    public void print(){
        System.out.println("LRU queue: ");
        Iterator i = fifo.iterator();
        while (i.hasNext()) {
            System.out.println(i.next());
        }
        System.out.println("LRU kvp: ");
        Set<Entry<String,String>> kvp_set = kvp_map.entrySet();
        for(Entry entry: kvp_set) {
            System.out.println("Key="+entry.getKey()+", Value="+entry.getValue());
        }
    }
}

