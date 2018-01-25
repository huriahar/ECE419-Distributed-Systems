package app_kvServer;

import java.net.Socket;
import java.net.ServerSocket;

import java.io.IOException;

import logger.LogSetup;

import cache.KVCache;
//import cache.KVCacheFIFO;
//import cache.KVCacheLRU;
//import cache.KVCacheLFU;
import common.messages.TextMessage;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class KVServer extends Thread implements IKVServer {

    private static Logger logger = Logger.getRootLogger();
    private int port;
    private ServerSocket serverSocket;
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
    public KVServer (int port, int cacheSize, String strategy) {
        this.port = port;
        if(cacheSize <= 0){
            logger.warn("Invalid cacheSize -> cache is null");
            this.cache = null;
        }
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

    /**
     * Main entry point for the KV server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if(args.length != 1) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <port>!");
            }
            else {
                int port = Integer.parseInt(args[0]);
                //new KVServer(port).start();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port>! Not a number!");
            System.out.println("Usage: Server <port>!");
            System.exit(1);
        }
    }
}
