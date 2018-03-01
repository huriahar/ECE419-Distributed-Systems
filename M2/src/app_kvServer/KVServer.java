package app_kvServer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.LinkedList;
import java.util.List;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.KVConstants;

import cache.IKVCache.CacheStrategy;
import cache.KVCache;

public class KVServer extends Thread implements IKVServer {

	private static Logger logger = Logger.getRootLogger();
    private int serverPort;
    private ServerSocket serverSocket;
    private KVCache cache;
    private boolean running;
    private String KVServerName ;
    private boolean locked;
    /**
	 * Start KV Server with selected name
	 * @param name			unique name of server
	 * @param zkHostname	hostname where zookeeper is running
	 * @param zkPort		port where zookeeper is running
	 */
	public KVServer(String name, String zkHostname, int zkPort) {
		// TODO Auto-generated method stub
        this.KVServerName = name;

        // TODO: Figure out how to get cache parameters
	}

	public int getPort(){
        return this.serverPort;
	}

	public String getHostname(){
        return this.KVServerName;
	}

	public CacheStrategy getCacheStrategy(){
        return this.cache.getStrategy();
	}

	public int getCacheSize(){
        return this.cache.getCacheSize();
	}

	public boolean inStorage(String key){
        //TODO check if server is responsible for key
        if (inCache(key)) return true;
        // We need to check if key is in permanent disk storage as well!
		String value = "";
		try {
			value = onDisk(key);
		}
		catch (IOException ex) {
			logger.error("ERROR: " + ex); 
		}
		if(value.equals(""))
			return false;	
        return true;
	}

	public boolean inCache(String key){
        return this.cache.hasKey(key);
	}

    public String getKV(String key) throws Exception{
        //TODO check if server is responsible for key
        //TODO what if asked for a KVpair that is not on disk
        String value = this.cache.getValue(key);
        if(value.equals("")){
            // 1- retrieve from disk    
            value = getValueFromDisk(key);            
            
            // 2 - insert in cache
            this.cache.insert(key, value);
        }
        this.cache.print();
        return value;
	}

    public void putKV(String key, String value) throws Exception{
        //TODO check that server is responsible for key
        this.cache.insert(key, value);
		storeKV(key, value);
        this.cache.print();
	}

    public void clearCache(){
        this.cache.clearCache();
	}

    public void clearStorage(){
        clearCache();

        File file = new File(this.KVServerName);
        file.delete();
	}

    public void run(){
        running = initializeServer();
        this.serverPort = serverSocket.getLocalPort();

        if (serverSocket != null) {
            while(isRunning()){
                try {
                    Socket client = serverSocket.accept();
                    ClientConnection connection = 
                            new ClientConnection(this, client);
                    new Thread(connection).start();

                    logger.info("Connected to " 
                            + client.getInetAddress().getHostName() 
                            +  " on port " + client.getPort());
                }
                catch (IOException e) {
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
        //TODO a server should be initialized with zookeeper information now
        logger.info("Initialize server ...");
        try {
            serverSocket = new ServerSocket(serverPort);
            logger.info("Server listening on port: " 
                    + serverSocket.getLocalPort());    
            return true;
        }
        catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + serverPort + " is already bound!");
            }
            return false;
        }
    }

	@Override
    public void kill(){
        running = false;
        try {
            serverSocket.close();
        }
        catch (IOException ex) {
            logger.error("Error! Unable to close socket on port: " + serverPort, ex);
        }
	}

	@Override
    public void close(){
        // TODO: Wait for all threads, save any remainder stuff in cache to memory
        running = false;
        try {
            serverSocket.close();
        }
        catch (IOException ex) {
            logger.error("Error! Unable to close socket on port: " + serverPort, ex);
        }
	}

    public String onDisk(String key) throws IOException {

		String value = "";
		String key_val, get_value;;
		String filePath  = this.KVServerName;
		BufferedReader br = null;
		String KVPair;
		try {
			File file = new File(filePath);
			
			FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
			FileLock lock = channel.lock();			

			try {
				lock = channel.tryLock();

    		} catch (OverlappingFileLockException e) {
				 //System.out.println("Overlapping File Lock Error: " + e.getMessage());
            }

		    if(!file.exists()) {
				System.out.println("File not found");
			}
			else{
				
				FileReader fr = new FileReader(file);
				br = new BufferedReader(fr);
				KVPair = br.readLine();
	
				while(KVPair != null) {
                    String[] msgContent = KVPair.split("\\" + KVConstants.DELIM);
                    key_val = msgContent[0];
                    List<String> valueParts = new LinkedList<>();
                    for (int i = 1; i < msgContent.length; ++i) {
                        valueParts.add(msgContent[i]);
                    }
                    get_value = String.join(KVConstants.DELIM, valueParts);
					
					if(key_val.equals(key)) {
						value = get_value;
						break; 	
					}
					KVPair = br.readLine();
				}
            }

			lock.release();
			channel.close();

		} catch (IOException ex) { 
                System.out.println("Unable to open file. ERROR: " + ex);
		
		} finally {
		    try{
		        if(br!=null)
		            br.close();
		
		    }   catch(Exception ex){
		            System.out.println("Error in closing the BufferedReader"+ex);
		    }
		}

		return value;		
	}

    public void storeKV(String key, String value) throws IOException {

        //TODO : Cache it in KVServer

		String filePath  = this.KVServerName;
		BufferedWriter wr  = null;
		PrintWriter pw = null;
		boolean toBeDeleted = false; 

		String curVal = onDisk(key);
		if(value == "") {
			toBeDeleted = true;
		}

		if(!curVal.equals("")) {
				//rewrite entire file back with new values
			writeNewFile(key, value, toBeDeleted);		
		}
		else{
			if(!toBeDeleted) {
				
				//simply append it to the end
				try {
				    File file = new File(filePath);
				
			
					FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
					FileLock lock = channel.lock();			
	
					try {
						lock = channel.tryLock();

    				} catch (OverlappingFileLockException e) {
						 //System.out.println("Overlapping File Lock Error: " + e.getMessage());
            		}

				    if (!file.exists()) {
				        file.createNewFile();
				    }
				
				    FileWriter fw = new FileWriter(file, true);
				    wr = new BufferedWriter(fw);
					pw = new PrintWriter(wr);
					String KVPair = key + "|" + value ;	
					pw.println(KVPair);

					lock.release();
					channel.close();

				} catch (IOException io) {
				
				    io.printStackTrace();
				}
					
				finally
				{
				    try{
				        if(wr!=null)
				            wr.close();
				
				    }   catch(Exception ex){
				            System.out.println("Error in closing the BufferedWriter"+ex);
				     }
				}
			}
		}
	}


	public void writeNewFile(String key, String value, boolean toDelete) {

		String key_val;
		String KVPair;
		String filePath  = this.KVServerName; 
		StringBuffer stringBuffer = new StringBuffer();			
		BufferedReader br = null;
		BufferedWriter wr  = null;
		String newPair = key + "|" + value ;
		
		try {
			File file = new File(filePath);
			
			FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
			FileLock lock = channel.lock();			
	
			try {
				lock = channel.tryLock();

    		} catch (OverlappingFileLockException e) {
				 //System.out.println("Overlapping File Lock Error: " + e.getMessage());
            }
			
		    if(!file.exists()) {
				System.out.println("File not found");
			}
			else{
			
				FileReader fr = new FileReader(file);
				br = new BufferedReader(fr);
				KVPair = br.readLine();
				while(KVPair != null) {

                    String[] msgContent = KVPair.split("\\" + KVConstants.DELIM);
                    key_val = msgContent[0];
                    List<String> valueParts = new LinkedList<>();
                    for (int i = 1; i < msgContent.length; ++i) {
                        valueParts.add(msgContent[i]);
                    }
                    value = String.join(KVConstants.DELIM, valueParts);

					if(key_val.equals(key)) {
						if(!toDelete) {
							stringBuffer.append(newPair + "\n");
						}
					}
					else{
						stringBuffer.append(KVPair + "\n");
					}
					KVPair = br.readLine();
				}

				br.close();
				String inputString = (stringBuffer.toString()).trim();

				FileWriter fw = new FileWriter(file);
			    wr = new BufferedWriter(fw);
				PrintWriter pw = new PrintWriter(wr);
				pw.println(inputString);			
				wr.close();	
            }

			lock.release();
			channel.close();

		} catch (IOException ex) { 
                System.out.println("Unable to open file. ERROR: " + ex);
		}

	}

    public String getValueFromDisk(String key) throws IOException {
	    return onDisk(key);
    }

    public void printCache() {
        this.cache.print();
    }


	@Override
	public void start() {
		// TODO Starts the KVServer, all client requests and all ECS requests are processed.
	}

    @Override
    public void stop() {
		// TODO Stops the KVServer, all client requests are rejected and only ECS requests are processed
	}

    @Override
    public void shutdown() {
		// TODO Exits the KVServer application.
	}
    

    @Override
    public void lockWrite() {
		// TODO Lock the KVServer for write operations.
        this.locked = true;
	}

    @Override
    public void unlockWrite() {
		// TODO Unlock the KVServer for write operations.
        this.locked = false;
	}

    public boolean isWriteLocked() {
        return this.locked;
    }

    @Override
    public boolean moveData(String[] hashRange, String targetName) throws Exception {
		// TODO Transfer a subset (range) of the KVServer's data to another KVServer (reallocation before
        // removing this server or adding a new KVServer to the ring); send a notification to the ECS,
        // if data transfer is completed.
		return false;
	}

    /**
     * Main entry point for the KV server application. 
     * @param args contains the port number at args[0]
     * cacheSize at args[1] and replacementPolicy at args[1]
     */
    public static void main (String[] args) {
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if(args.length < 2 || args.length > 3) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <port> <cacheSize> [<replacementPolicy>]!");
            }
            else {
                int port = Integer.parseInt(args[0]);
                int cacheSize = Integer.parseInt(args[1]);
                String replacementPolicy = "";
                // Cache Replacement policy is supplied
                if (args.length == 3) {
                    replacementPolicy = args[2];
                }
                new KVServer(port, cacheSize, replacementPolicy).start();
            }
        }
        catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
        catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port> or <cacheSize>! Not a number!");
            System.out.println("Usage: Server <port> <cacheSize> [<replacementPolicy>]!");
            System.exit(1);
        }
    }

	@Override
	public void deleteKV(String key) throws Exception {
		// TODO Auto-generated method stub
		
	}
}
