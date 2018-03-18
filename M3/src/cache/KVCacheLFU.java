package cache;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

public class KVCacheLFU extends KVCache {

    private Map<String, String>  kvp_map = new HashMap();
    private Map<String, Integer> kfp_map = new HashMap();

    public KVCacheLFU(int cacheSize) {
        super(cacheSize, "LFU");
    }

    private synchronized String findLFU(){
        Iterator<Map.Entry<String,Integer>> map_it = kfp_map.entrySet().iterator();

        //Iterate over the frequency hashmap, find the lowest value
        Map.Entry<String, Integer> lfu = map_it.next();
        while (map_it.hasNext()){
            Map.Entry<String, Integer> next = map_it.next();

            if (next.getValue() < lfu.getValue()) {
                lfu = next;
            }
        }
        return lfu.getKey();
    }

    @Override
    public synchronized void insert(String key, String value){
        if(value.equals("")) return;
        if(this.getCacheSize() == 0) return;
        // check if it already exists
        if (kvp_map.containsKey(key)) {
            //update the value
            kvp_map.put(key, value);
            kfp_map.put(key, kfp_map.get(key) + 1);
            return;
        }
            
        // if it doesn't exist, cache it
        // if the cache is full, evict according to replacement policy
        if(kvp_map.size() == this.getCacheSize()) {
            //LFU replacement            
            String lfu = findLFU();

            kfp_map.remove(lfu);
            kvp_map.remove(lfu);
        }
        kvp_map.put(key, value);
        kfp_map.put(key, 1);
    }

    @Override
    public synchronized void delete(String key){
        if( kvp_map.containsKey(key)) {
            kvp_map.remove(key);
        }
        if( kfp_map.containsKey(key)) {
            kfp_map.remove(key);
        }
    }

    @Override
    public synchronized boolean hasKey(String key){
        return kvp_map.containsKey(key);
    }

    @Override
    public synchronized String getValue(String key){
        if(hasKey(key)) {
            kfp_map.put(key, kfp_map.get(key) + 1);
            return kvp_map.get(key);
        }
        return "";
    }

    @Override
    public synchronized void clearCache(){
        kvp_map.clear();
        kfp_map.clear();
    }

    @Override
    public void print() {
        System.out.println("LFU kvp: ");
        Set<Entry<String,String>> kvp_set = kvp_map.entrySet();
        for(Entry entry: kvp_set) {
            System.out.println("Key="+entry.getKey()+", Value="+entry.getValue());
        }

        System.out.println("LFU kfp: ");
        Set<Entry<String,Integer>> kfp_set = kfp_map.entrySet();
        for(Entry entry: kfp_set) {
            System.out.println("Key="+entry.getKey()+", Value="+entry.getValue());
        }
    }
}

