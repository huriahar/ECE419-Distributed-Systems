package cache;

import app_kvServer.IKVServer;

public class KVCache implements IKVCache {
    private int cacheSize;
    private IKVServer.CacheStrategy strategy;


    public KVCache(int cacheSize, String strategy) {
        this.cacheSize = cacheSize;
        this.strategy = cacheStrategyStrToEnum(strategy);
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
}
