package cache;

public class KVCache implements IKVCache {
    private static int cacheSize;
    private static CacheStrategy strategy;

    public KVCache(int cacheSize, String strategy){
        KVCache.cacheSize = cacheSize;
        KVCache.strategy = CacheStrategy.valueOf(strategy);
    }
    
    public static boolean isValidStrategy(String strategy) {
    	for (CacheStrategy c : CacheStrategy.values()) {
    		if (c.name().equals(strategy)) {
    			return true;
    		}
    	}
    	return false;
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
    public CacheStrategy getStrategy(){
        return KVCache.strategy;
    }

    @Override
    public int getCacheSize(){
        return KVCache.cacheSize;
    }

    private CacheStrategy cacheStrategyStrToEnum(String strategy) {
        switch(strategy) {
            case "FIFO":
                return CacheStrategy.FIFO;
            case "LRU":
                return CacheStrategy.LRU;
            case "LFU":
                return CacheStrategy.LFU;
            default:
                return CacheStrategy.None;
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
