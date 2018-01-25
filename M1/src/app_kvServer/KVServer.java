package app_kvServer;

import java.net.Socket;
import java.net.ServerSocket;
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
        createCache(cacheSize, strategy);
    }

    private void createCache (int cacheSize, String strategy) {
        if (cacheSize <= 0) {
            logger.warn("Invalid cacheSize -> cache is null");
            this.cache = null;
        }
        /*else if (strategy.equals("FIFO")) {
            logger.info("Creating FIFO cache with size " + cacheSize);
            this.cache = new KVCacheFIFO(cacheSize, strategy);
        }
        else if (strategy.equals("LRU")) {
            logger.info("Creating LRU cache with size " + cacheSize);
            this.cache = new KVCacheLRU(cacheSize, strategy);
        }
        else if (strategy.equals("LFU")) {
            logger.info("Creating LFU cache with size " + cacheSize);
            this.cache = new KVCacheLFU(cacheSize, strategy);
        }
        else {
            logger.warn("Invalid cache replacement strategy -> cache is null");
            this.cache = null;
        }*/
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
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean inCache(String key){
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getKV(String key) throws Exception{
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public void putKV(String key, String value) throws Exception{
        // TODO Auto-generated method stub
    }

    @Override
    public void clearCache(){
        // TODO Auto-generated method stub
    }

    @Override
    public void clearStorage(){
        // TODO Auto-generated method stub
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        running = initializeServer();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    ClientConnection connection = new ClientConnection(client);
                    new Thread(connection).start();

                    logger.info("Connected to " + client.getInetAddress().getHostName() + 
                        " on port " + client.getPort());
                }
                catch (IOException e) {
                    logger.error("Error! " + "Unable to establish connection. \n", e);
                }
            }
        }
        logger.error("Server stopped.");
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
