package app_kvServer;

import java.net.Socket;
import java.net.ServerSocket;
import java.io.IOException;
import java.net.UnknownHostException;
import java.net.BindException;
import java.net.InetAddress;

import java.nio.file.Files;
import java.io.FileWriter;  
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import logger.LogSetup;

import cache.*;
import common.messages.TextMessage;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class KVServer extends Thread implements IKVServer {

    private static Logger logger = Logger.getRootLogger();
    private int serverPort;
    private ServerSocket serverSocket;
    private KVCache cache;
    private boolean running;
    private String KVServerName ;


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
        this.serverPort = port;
        this.KVServerName = "Server_" + String.valueOf(port);
        if (cacheSize <= 0) {
            logger.warn("Invalid cacheSize -> cache is null");
            this.cache = null;
        }
        else 
            this.cache = KVCache.createKVCache(cacheSize, strategy);
    }

    @Override
    public int getPort() {
        return this.serverPort;
    }

    @Override
    public String getHostname() {
        // TODO Auto-generated method stub
        if (serverSocket == null) return "";
        try {
            return serverSocket.getInetAddress().getLocalHost().getHostAddress();
        }
        catch (UnknownHostException ex) {
            logger.error("Unknown Host! Unable to get Hostname");
            return "";
        }
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        return this.cache.getStrategy();
    }

    @Override
    public int getCacheSize() {
        return this.cache.getCacheSize();
    }

    @Override
    public boolean inStorage(String key) {
        if (inCache(key)) return true;
        // TODO: We need to check if key is in permanant disk storage as well!
        return false;
    }

    @Override
    public boolean inCache(String key) {
        return this.cache.hasKey(key);
    }

    @Override
    public String getKV(String key)
            throws Exception {
        String value = this.cache.getValue(key);
        if(value.equals("")){
            // 1- retrieve from disk    
            // TODO
            value = getValue(key);            
            
            // 2 - insert in cache
            this.cache.insert(key, value);
        }
        this.cache.print();
        return value;
    }

    @Override
    public void putKV(String key, String value)
            throws Exception {
        //TODO write in storage
        this.cache.insert(key, value);
        this.cache.print();
		storeKV(key, value);
    }

    @Override
    public void deleteKV(String key)
            throws Exception {
        this.cache.delete(key);
        this.cache.print();
    }

    @Override
    public void clearCache() {
        this.cache.clearCache();
    }

    @Override
    public void clearStorage() {
        // TODO Auto-generated method stub
        clearCache();
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        running = initializeServer();
        this.serverPort = serverSocket.getLocalPort();

        if (serverSocket != null) {
            while(isRunning()){
                try {
                    Socket client = serverSocket.accept();
                    ClientConnection connection = 
                            new ClientConnection(this, client,  this.cache);
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
    public void kill() {
        // TODO Auto-generated method stub
        running = false;
        try {
            serverSocket.close();
        }
        catch (IOException ex) {
            logger.error("Error! Unable to close socket on port: " + serverPort, ex);
        }
    }

    @Override
    public void close() {
        // TODO Auto-generated method 
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
		    if(!file.exists()) {
				System.out.println("File not found");
			}
			else{
				
				FileReader fr = new FileReader(file);
				br = new BufferedReader(fr);
				System.out.println("File found. Beginning to read");
				KVPair = br.readLine();
	
				while(KVPair != null) {
					key_val = KVPair.split("|")[0];
					get_value 	= KVPair.split("|")[2];
					System.out.println("Value: " + get_value + " and key: " + key_val);	
					if(key_val.equals(key)) {
						value = get_value;
						break; 	
					}
					KVPair = br.readLine();
				}
            }
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
		
		System.out.println(filePath);
		BufferedWriter wr  = null;
		PrintWriter pw = null;
		String curVal = onDisk(key);
		if(curVal != "") {
				//rewrite entire file back with new values
			if(value == "") {
				writeNewFile(key, value, true);				
				System.out.println("Existing Pair: " + key);
			}
			else {
				writeNewFile(key, value, false);
				System.out.println("Deleting Pair: " + key );
			}
		}
		else{
			//simply append it to the end
			System.out.println("New KKV Pair");
			try {
			    File file = new File(filePath);
			
			    if (!file.exists()) {
			        file.createNewFile();
			    }
			
			    FileWriter fw = new FileWriter(file, true);
			    wr = new BufferedWriter(fw);
				pw = new PrintWriter(wr);
				String KVPair = key + "|" + value ;	
				pw.println(KVPair);

			} catch (IOException io) {
			
			    io.printStackTrace();
			}
				
			finally
			{
			    try{
			        if(wr!=null)
			            wr.close();
//					if(pw != null)
//						pw.close();
			
			    }   catch(Exception ex){
			            System.out.println("Error in closing the BufferedWriter"+ex);
			     }
			}
		}
	}


	public void writeNewFile(String key, String value, boolean toDelete) {

		String key_val = "";
		String KVPair = "Empty";
		String filePath  = this.KVServerName; 
		System.out.println("Write to File. Delete:" + toDelete);
		StringBuffer stringBuffer = new StringBuffer();			
		BufferedReader br = null;
		BufferedWriter wr  = null;
		String newPair = key + "|" + value ;
		String newline = System.getProperty("line.separator");
		
		try {
			File file = new File(filePath);
		    if(!file.exists()) {
				System.out.println("file not found");
			}
			else{
			
				FileReader fr = new FileReader(file);
				br = new BufferedReader(fr);
				System.out.println("Reading File");	
				KVPair = br.readLine();
				System.out.println("Initial KVpair Read in : " + KVPair);
				while(KVPair != null) {
					key_val = KVPair.split("|")[0];
					value 	= KVPair.split("|")[2];
					System.out.println("Value: " + value + " and key: " + key_val);
					if(key_val.equals(key)) {
						if(toDelete) {
							System.out.println("Assume it's deleled");
						}							
						else {
							System.out.println("Adding in updatedPair");
							stringBuffer.append(newPair);
							System.out.println(stringBuffer);
						}
					}
					else{
						System.out.println("Add in  old Pair with no changes");
						stringBuffer.append(KVPair);
						System.out.println(stringBuffer);

					}
					KVPair = br.readLine();
					if(KVPair != null )
						stringBuffer.append("\n");
//						System.out.println("Newline char: " + KVPair.contains(newline));
					System.out.println("KVpair Read in : " + KVPair);
				}

				System.out.println("Exited loop ");
				br.close();


				System.out.println("Writing the sb to file");
				FileWriter fw = new FileWriter(file);
			    wr = new BufferedWriter(fw);
				PrintWriter pw = new PrintWriter(wr);
				pw.println(stringBuffer.toString());			
				wr.close();	
//				pw.close();
            }
		} catch (IOException ex) { 
                System.out.println("Unable to open file. ERROR: " + ex);
		}

	}

    public String getValue(String key) throws IOException {
	    return onDisk(key);
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
}
