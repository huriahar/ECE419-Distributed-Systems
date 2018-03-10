package app_kvServer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;

import java.math.BigInteger;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.LinkedList;
import java.util.ArrayList;
import java.util.List;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import common.md5;
import common.KVConstants;
import common.ServerMetaData;
import common.messages.TextMessage;
import cache.IKVCache.CacheStrategy;
import cache.KVCache;
import client.KVStore;
import ecs.ZKImplementation;

public class KVServer implements IKVServer, Runnable {

    private static Logger logger = Logger.getRootLogger();
    //Server tools
    private ServerSocket serverSocket;
    private KVCache cache;
    //Metadata
    private ServerMetaData metadata;
    private Path metaDataFile;
    private String serverFilePath;
    private String zkPath;
    //State
    private boolean running = false;
    private boolean writeLocked = true;     //start in a stopped state
    private boolean readLocked = true;      //start in a stopped state
    private boolean moveAll = false;
    //Default values
    private static final String UNSET_ADDR = null;
    private static final int DEFAULT_CACHE_SIZE = 5;
    private static final String DEFAULT_CACHE_STRATEGY = "FIFO";
    
    private ZKImplementation zkImplServer;
    /**
     * Start KV Server with selected name
     * @param name            unique name of server
     * @param zkHostname    hostname where zookeeper is running
     * @param zkPort        port where zookeeper is running
     */
    public KVServer(String name, String zkHostname, int zkPort) {
    	zkImplServer = new ZKImplementation();
    	String data = null;
    	try {
			zkImplServer.zkConnect(zkHostname);
			this.zkPath = KVConstants.ZK_SEP + KVConstants.ZK_ROOT + KVConstants.ZK_SEP + name;
			data = zkImplServer.readData(this.zkPath);			
		} catch (IOException | InterruptedException e) {
			logger.error("Unable to connect to zk and read data: " + e);
		} catch (KeeperException e) {
			logger.error("Unable to connect to zk and read data: " + e);
		}
    	String[] zNodeData = data.split("\\" + KVConstants.DELIM);
    	this.metadata = new ServerMetaData(name, zNodeData[1], Integer.parseInt(zNodeData[2]), null, null);
        this.serverFilePath = "SERVER_" + Integer.toString(zkPort);
        this.cache = KVCache.createKVCache(0, "FIFO");
        this.metaDataFile = Paths.get("metaDataECS.config");
        /////////////////////// 
        //Update ZK node status
        /////////////////////// 
        data = "SERVER_LAUNCHED" + KVConstants.DELIM + this.metadata.addr + KVConstants.DELIM + this.metadata.port;
        try {
            zkImplServer.updateData(this.zkPath, data);
        } catch (KeeperException e) {
        	logger.error("ERROR: Unable to update ZK " + e);
        } catch (InterruptedException e) {
        	logger.error("ERROR: ZK Interrupted" + e);
        }
     }

    public int getPort(){
        return this.metadata.getServerPort();
    }

    public String getHostname(){
        return this.metadata.getServerName();
    }
    
    public String getHostAddr() {
    	return this.metadata.getServerAddr();
    }

    public CacheStrategy getCacheStrategy(){
        return this.cache.getStrategy();
    }

    public int getCacheSize(){
        return this.cache.getCacheSize();
    }

    public void setupCache(int size, String strategy) {
        this.cache = KVCache.createKVCache(size, strategy);
    }

    public void setMoveAll(boolean move) {
        this.moveAll = move;
    }

    public boolean inStorage(String key) {
        if (inCache(key)) return true;
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
        //TODO what if asked for a KVpair that is not on disk
        String value = this.cache.getValue(key);
        if(value.equals("")){
            // 1- retrieve from disk    
            value = getValueFromDisk(key);            
            if(!value.equals("")) {
                // 2 - insert in cache
                this.cache.insert(key, value);
            } 
        }
        //this.cache.print();
        return value;
    }

    public void putKV(String key, String value) throws Exception{
        this.cache.insert(key, value);
        //this.cache.print();
        storeKV(key, value);
    }

    public void clearCache(){
        this.cache.clearCache();
    }

    public void clearStorage(){
        clearCache();

        File file = new File(this.serverFilePath);
        file.delete();
    }

    public ServerMetaData getMetaData() {
        return this.metadata;
    }

    @Override
    public void run(){
        running = initializeServer();
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
        logger.info("Initialize server ...");
        try {
            serverSocket = new ServerSocket(metadata.getServerPort());
            logger.info("Server listening on port: " 
                    + serverSocket.getLocalPort());    
            return true;
        }
        catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + this.metadata.port + " is already bound!");
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
            logger.error("Error! Unable to close socket on port: " + metadata.port, ex);
        }
    }

    @Override
    public void close(){
        // TODO: Wait for all threads, save any remainder stuff in cache to memory
        try {
            serverSocket.close();
        }
        catch (IOException ex) {
            logger.error("Error! Unable to close socket on port: " + metadata.port, ex);
        }
    }

    public String onDisk(String key) throws IOException {
        String value = "";
        String key_val, get_value;
        String filePath  = this.serverFilePath;
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
                    if(KVPair.trim().length() == 0) {
                        KVPair = br.readLine();
                        continue;
                    }
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

    @Override
    public void deleteKV(String key) throws Exception {
        this.cache.delete(key);
        storeKV(key, "");
    }


    public void storeKV(String key, String value) throws IOException {
        String filePath  = this.serverFilePath;
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
                        if(wr!=null) {
                            wr.close();
                        }
                
                    } catch(Exception ex){
                        System.out.println("Error in closing the BufferedWriter"+ex);
                    }
                }
            }
        }
    }


    public void writeNewFile(String key, String value, boolean toDelete) {

        String key_val;
        String KVPair;
        String filePath  = this.serverFilePath; 
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

    public boolean isResponsible(String key) {
        BigInteger encodedKey = md5.encode(key);
        System.out.println("DEBUG: key " + encodedKey + " bHash " + metadata.bHash + " eHash " + metadata.eHash);
        System.out.println("DEBUG: minHash " + KVConstants.MIN_HASH + " maxHash " + KVConstants.MAX_HASH);
        boolean ret = false, ret2 = false, ret3 = false;
        if( (metadata.bHash).compareTo(metadata.eHash) < 0 ) {
            ret = (encodedKey.compareTo(metadata.bHash) >= 0  && encodedKey.compareTo(metadata.eHash) < 0);
            System.out.println("DEBUG: isResp? " + ret);
            return ret;
        } else {
            ret2 = (encodedKey.compareTo(metadata.bHash) >= 0 && encodedKey.compareTo(KVConstants.MAX_HASH) < 0);
            ret3 = (encodedKey.compareTo(KVConstants.MIN_HASH) >= 0 && encodedKey.compareTo(metadata.eHash) < 0);
            System.out.println("DEBUG: isResp? " + ret2);
            System.out.println("DEBUG: isResp? " + ret3);
            return (ret2 || ret3);
        }
    }

    public String getMetaDataFromFile() {
        StringBuilder marshalledData = new StringBuilder();
        try {
            ArrayList<String> metaData = new ArrayList<>(Files.readAllLines(this.metaDataFile,
                                                         StandardCharsets.UTF_8));
            for(String line : metaData) {
                marshalledData.append(line + KVConstants.NEWLINE_DELIM);
            }
        } catch (IOException e) {
            marshalledData.append("METADATA_FETCH_ERROR");
            logger.error("METADATA_FETCH_ERROR could not fetch meta data: " + e);
        }
        return marshalledData.toString();
    }

    public String getMetaDataOfServer(String hostName) {
    	ArrayList<String> metaDataLines = null;
		try {
			metaDataLines = new ArrayList<>(Files.readAllLines(this.metaDataFile, StandardCharsets.UTF_8));
		}
		catch (IOException e) {
			logger.error("METADATA_FETCH_ERROR could not fetch meta data: " + e);
		}
    	for (int i = 0; i < metaDataLines.size(); ++i) {
    		String[] metaData = metaDataLines.get(i).split("\\" + KVConstants.DELIM);
    		if (metaData[ServerMetaData.SERVER_NAME].equals(hostName)) {
    			return metaDataLines.get(i);
    		}
    	}
        logger.error("Error: could not find metadata for server \"" + getHostname());
        return null;
    }

    public boolean updateMetaData() {
        String meta = getMetaDataOfServer(getHostname());
        if(meta == null) {
            System.out.println("Could not find meta data of server " + getHostname() + " " + getPort());
            return false;
        }
        metadata = new ServerMetaData(meta);
        logger.info("Set KVServer (" + metadata.getServerName() + ", " + metadata.getServerAddr() + ", " + metadata.getServerPort() + ") " +
                    "\nStart hash to: " + metadata.getBeginHash().toString(16) + "\nEnd hash to: " + metadata.getEndHash().toString(16));
        return true;
    }

    @Override
    public void start() {
        writeLocked = false;
        readLocked = false;
        String data = "SERVER_STARTED" + KVConstants.DELIM + this.metadata.addr + KVConstants.DELIM + this.metadata.port;
        try {
            zkImplServer.updateData(this.zkPath, data);
        } catch (KeeperException e) {
        	logger.error("ERROR: Unable to update ZK " + e);
        } catch (InterruptedException e) {
        	logger.error("ERROR: ZK Interrupted" + e);
        }
    }

    @Override
    public void stop() {
        writeLocked = true;
        readLocked = true;
        String data = "SERVER_STOPPED" + KVConstants.DELIM + this.metadata.addr + KVConstants.DELIM + this.metadata.port;
        try {
            data = zkImplServer.readData(this.zkPath);			
            zkImplServer.updateData(this.zkPath, data);
        } catch (KeeperException e) {
        	logger.error("ERROR: Unable to update ZK " + e);
        } catch (InterruptedException e) {
        	logger.error("ERROR: ZK Interrupted" + e);
        } catch (Exception e) {
        	logger.error("ERROR: ZK Exception" + e);
        }
    }

    @Override
    public void shutdown() {
        writeLocked = true;
        readLocked = true;
        running = false;
        String data = "SERVER_SHUTDOWN" + KVConstants.DELIM + this.metadata.addr + KVConstants.DELIM + this.metadata.port;
        try {
            zkImplServer.updateData(this.zkPath, data);
        } catch (KeeperException e) {
        	logger.error("ERROR: Unable to update ZK " + e);
        } catch (InterruptedException e) {
        	logger.error("ERROR: ZK Interrupted" + e);
        }
        this.close();
    }

    @Override
    public void lockWrite() {
        this.writeLocked = true;
    }

    @Override
    public void unlockWrite() {
        this.writeLocked = false;
    }

    public boolean isWriteLocked() {
        return this.writeLocked;
    }

    public boolean isReadLocked() {
        return this.readLocked;
    }

    public boolean isStopped() {
        return (this.isWriteLocked() && this.isReadLocked());
    }

    @Override
    public boolean moveData(String[] hashRange, String targetName) 
    	throws Exception {
        // TODO Transfer a subset (range) of the KVServer's data to another KVServer (reallocation before
        // removing this server or adding a new KVServer to the ring); send a notification to the ECS,
        // if data transfer is completed.
        System.out.println("DEBUG: getHostname() = " + getHostname());
        System.out.println("DEBUG: targetName() = " + targetName);
        if(targetName.equals(getHostname())) return true;
        lockWrite();
        StringBuilder toSend = new StringBuilder();
        toSend.append("MOVE_KVPAIRS" + KVConstants.DELIM);
        boolean found = false;
        boolean success = true;
        ArrayList<String> toDelete = new ArrayList<>(); 
        try {
            Path serverPath = Paths.get(this.serverFilePath);
            if(Files.exists(serverPath)) {
                ArrayList<String> keyValuePairs = new ArrayList<>(Files.readAllLines(serverPath,
                                                             StandardCharsets.UTF_8));
                for(String line : keyValuePairs) {
                    //TODO this is because some white spaces get inserted in the file
                    //this if statement is a temporary workaround for that
                    if (line.trim().length() == 0) continue; 
                    String[] kvp = line.split("\\" + KVConstants.DELIM);
                    if(this.moveAll || !isResponsible(kvp[0])) {
                        logger.debug(getHostname() + " not responsible for key " + kvp[0]);
                        found = true;
                        toSend.append(line + KVConstants.NEWLINE_DELIM);
                        toDelete.add(kvp[0]);
                    }
                    else {
                        logger.debug(getHostname() + " responsible for key " + kvp[0]);
                    }
                }
            }
            else {
                serverPath = Files.createFile(serverPath);
            }
        } catch (IOException e) {
            logger.error("ERROR while moving data from server to target server" + targetName);
            return false;
        }
        if(found) {
            //Send to receiving server
            ServerMetaData targetMeta = new ServerMetaData(getMetaDataOfServer(targetName));
            KVStore sender = new KVStore(targetMeta.addr, targetMeta.port);
            sender.connect();
            sender.sendMessage(new TextMessage(toSend.toString()));
            TextMessage reply = sender.receiveMessage();
            sender.disconnect();
            success = (reply.getMsg().equals("MOVE_SUCCESS"));
            if(success) {
                for(String key: toDelete) {
                    deleteKV(key);
                }

            }
        } else {
            logger.debug("No data to move from " + getHostname());
        }
        unlockWrite();
        return success;
    }

    /**
     * Main entry point for the KV server application. 
     * @param args contains the port number at args[0]
     * cacheSize at args[1] and replacementPolicy at args[1]
     */
    public static void main (String[] args) {
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if(args.length != 3) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <name> <addr> <port>!");
            }
            else {
                String name = args[0];
                String addr = args[1];
                int port = Integer.parseInt(args[2]);
                new KVServer(name, addr, port).run();
            }
        }
        catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
        catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port>! Not a number!");
            System.out.println("Usage: Server <name> <port>!");
            System.exit(1);
        }
    }

}
