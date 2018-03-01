package cache;

public interface IKVCache {
	
	public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    };
    
    public int getCacheSize();

    public CacheStrategy getStrategy();

    public void insert(String key, String value);

    public void delete(String key);

    public boolean hasKey(String key);

    public String getValue(String key);

    public void clearCache();

    public void print();
}

