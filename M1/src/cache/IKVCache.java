package cache;

import app_kvServer.IKVServer;

public interface IKVCache {
    public int getCacheSize();

    public IKVServer.CacheStrategy getStrategy();

    public void insert(String key, String value);

    public void delete(String key);

    public boolean hasKey(String key);

    public String getValue(String key);

    public void clearCache();
}

