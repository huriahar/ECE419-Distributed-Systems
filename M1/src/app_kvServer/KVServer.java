package app_kvServer;

import cache.KVCache;
import org.apache.log4j.Logger;

public class KVServer implements IKVServer {

    private static Logger logger = Logger.getRootLogger();
    private int port;
    private KVCache cache;

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
        this.port = port;
        if(cacheSize == 0) this.cache = null;
        else this.cache = KVCache.createKVCache(cacheSize, strategy);
	}

	@Override
	public int getPort(){
        return this.port;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		return null;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
        return this.cache.getStrategy();
	}

	@Override
    public int getCacheSize(){
        return this.cache.getCacheSize();
	}

	@Override
    public boolean inStorage(String key){
        if(inCache(key)) return true;
        //TODO check if it is in storage (but not in cache)
		return false;
	}

	@Override
    public boolean inCache(String key){
        return this.cache.hasKey(key);
	}

	@Override
    public String getKV(String key) throws Exception{
        String value = this.cache.getValue(key);
        if(value.equals("")){
            // 1- retrieve from disk
            // TODO
            // 2 - insert in cache
            this.cache.insert(key, value);
        }
		return value;
	}

	@Override
    public void putKV(String key, String value) throws Exception{
        //TODO write in storage
        this.cache.insert(key, value);
	}

	@Override
    public void clearCache(){
        this.cache.clearCache();
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
	}

	@Override
    public void run(){
		// TODO Auto-generated method stub
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
	}
}
