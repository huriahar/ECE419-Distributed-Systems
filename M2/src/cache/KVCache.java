package cache;

import app_kvServer.IKVServer;

public class KVCache implements IKVCache {
    private static int cacheSize;
    private static IKVServer.CacheStrategy strategy;

    public KVCache(int cacheSize, String strategy){
        this.cacheSize = cacheSize;
        this.strategy = cacheStrategyStrToEnum(strategy);
    }

    public static KVCache createKVCache(int cacheSize, String strategy) {
        //TODO add other types of cache
        switch(strategy) {
            case "FIFO":
                return new KVCacheFIFO(cacheSize);
            case "LRU":
                return new KVCacheLRU(cacheSize);
            case "LFU":
                return new KVCacheLFU(cacheSize);
            default:
                return new KVCacheFIFO(cacheSize);
        }
    }

    @Override
    public IKVServer.CacheStrategy getStrategy(){
        return this.strategy;
    }

    @Override
    public int getCacheSize(){
        return this.cacheSize;
    }

    private IKVServer.CacheStrategy cacheStrategyStrToEnum(String strategy) {
        switch(strategy) {
            case "FIFO":
                return IKVServer.CacheStrategy.FIFO;
            case "LRU":
                return IKVServer.CacheStrategy.LRU;
            case "LFU":
                return IKVServer.CacheStrategy.LFU;
            default:
                return IKVServer.CacheStrategy.None;
        } 
    }

    @Override
    public synchronized void insert(String key, String value){
    }

    @Override
    public  synchronized void delete(String key){
    }

    @Override
    public synchronized boolean hasKey(String key){
        return false;
    }

    @Override
    public synchronized String getValue(String key){
        return "";
    }

    @Override
    public synchronized void clearCache(){
    }

    @Override
    public void print(){
    }
}
