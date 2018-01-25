package cache;

import app_kvServer.IKVServer;

public class KVCacheFIFO extends KVCache {
    private int cacheSize;
    private IKVServer.CacheStrategy strategy;

    public KVCacheFIFO(int cacheSize, String strategy) {
        super(cacheSize, strategy);
    }

}
