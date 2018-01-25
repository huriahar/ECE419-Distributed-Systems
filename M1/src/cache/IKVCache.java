package cache;

import app_kvServer.IKVServer;

public interface IKVCache {

    public int getCacheSize();

    public IKVServer.CacheStrategy getStrategy();

}

