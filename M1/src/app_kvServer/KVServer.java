package app_kvServer;

import java.net.Socket;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;

import java.io.IOException;
import java.net.BindException;

import logger.LogSetup;

import cache.*;
import common.messages.TextMessage;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class KVServer extends Thread implements IKVServer {

    private static Logger logger = Logger.getRootLogger();
    private int port;
    private ServerSocket serverSocket;
    private KVCache cache;
    private boolean running;
	private Path storagePath;

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
		this.storagePath = Paths.get(String.valueOf(port));
        if (cacheSize <= 0) {
            logger.warn("Invalid cacheSize -> cache is null");
            this.cache = null;
        }
        else 
            this.cache = KVCache.createKVCache(cacheSize, strategy);
	}

    @Override
    public int getPort(){
        return this.port;
    }

    @Override
    public String getHostname() {
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
    public void run() {
        // TODO Auto-generated method stub
    	running = initializeServer();
        
        if(serverSocket != null) {
	        while(isRunning()){
	            try {
	                Socket client = serverSocket.accept();                
	                ClientConnection connection = 
	                		new ClientConnection(client, this.storagePath, this.cache);
	                new Thread(connection).start();
	                
	                logger.info("Connected to " 
	                		+ client.getInetAddress().getHostName() 
	                		+  " on port " + client.getPort());
	            } catch (IOException e) {
	            	logger.error("Error! " +
	            			"Unable to establish connection. \n", e);
	            }
	        }
        }
        logger.info("Server stopped.");
    }

    private boolean isRunning() {
        return this.running;
    }

    private boolean initializeServer() {
        logger.info("Initialize server ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
                    + serverSocket.getLocalPort());    
            return true;
        }
        catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    @Override
    public void kill() {
        // TODO Auto-generated method stub
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }


    /**
     * Main entry point for the KV server application. 
     * @param args contains the port number at args[0]
     * cacheSize at args[1] and replacementPolicy at args[1]
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if(args.length != 3) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <port> <cacheSize> <replacementPolicy>!");
            }
            else {
                int port = Integer.parseInt(args[0]);
                int cacheSize = Integer.parseInt(args[1]);
                String replacementPolicy = args[2];
                new KVServer(port, cacheSize, replacementPolicy).start();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port> or <cacheSize>! Not a number!");
            System.out.println("Usage: Server <port> <cacheSize> <replacementPolicy>!");
            System.exit(1);
        }
    }
}
