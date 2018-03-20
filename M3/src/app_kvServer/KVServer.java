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
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
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
    private String currFilePath;
    private String serverFilePath;
    private String pReplicaFilePath;
    private String sReplicaFilePath;
    private String zkPath;
    // TimeStamper
    private TimeStamper timeStamper;
    // This server's replicas
    private ServerMetaData primaryReplica;
    private ServerMetaData secondaryReplica;
    //State
    private boolean running = false;
    private boolean writeLocked = true;     //start in a stopped state
    private boolean readLocked = true;      //start in a stopped state
    private boolean moveAll = false;
    private boolean pReplica = false;
    private boolean sReplica = false;
    
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
        this.pReplicaFilePath = "SERVER_" + Integer.toString(zkPort) + "_PRIMARY";
        this.sReplicaFilePath = "SERVER_" + Integer.toString(zkPort) + "_SECONDARY";
        // Delete Replica files, if they exist
        try {
            Files.deleteIfExists(Paths.get(pReplicaFilePath));
            Files.deleteIfExists(Paths.get(sReplicaFilePath));
        } catch (IOException ex) {
            // File permission problems are caught here.
            logger.error("Permission problems " + ex);
        }
        this.currFilePath = this.serverFilePath;
        this.cache = KVCache.createKVCache(0, "FIFO");
        this.metaDataFile = Paths.get("metaDataECS.config");

        this.primaryReplica = null;
        this.secondaryReplica = null;
        /////////////////////// 
        //Update ZK node status
        /////////////////////// 
        try {
            zkImplServer.updateData(this.zkPath, getZnodeData("SERVER_LAUNCHED", getCurrentTimeString()));
        } catch (KeeperException e) {
            logger.error("ERROR: Unable to update ZK " + e);
        } catch (InterruptedException e) {
            logger.error("ERROR: ZK Interrupted" + e);
        }
        this.timeStamper = new TimeStamper(this.zkImplServer, this.zkPath);
        new Thread(timeStamper).start();
     }

    private String getZnodeData(String status, String timestamp) {
        return status + KVConstants.DELIM + this.metadata.addr + KVConstants.DELIM + this.metadata.port + KVConstants.DELIM + timestamp;
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

    public String getServerFilePath(){
        return this.serverFilePath;
    }

    public String getPReplicaFilePath(){
        return this.pReplicaFilePath;
    }

    public String getSReplicaFilePath(){
        return this.sReplicaFilePath;
    }

    public String getCurrFilePath(){
        return this.currFilePath;
    }

    public void setCurrFilePath(String path){
        this.currFilePath = path;
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

                   updateTimeStamp(); 

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
                 //logger.error("Overlapping File Lock Error: " + e.getMessage());
            }

            if(!file.exists()) {
                logger.error("File not found");
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
                logger.error("Unable to open file. ERROR: " + ex);
        
        } finally {
            try{
                if(br!=null)
                    br.close();
        
            }   catch(Exception ex){
                    logger.error("Error in closing the BufferedReader"+ex);
            }
        }

        return value;
    }

    @Override
    public void deleteKV(String key) throws Exception {
        this.cache.delete(key);
        storeKV(key, "");
    }

    public boolean handleMoveKVPairs (String destination, String KVPairs) {
        if (KVPairs == null) return true;
        boolean success = true;
        if (destination.equals(KVConstants.PREPLICA)) {
            // DELETE THE current pReplica file
            try {
                Files.deleteIfExists(Paths.get(pReplicaFilePath));
            } catch (IOException ex) {
                // File permission problems are caught here.
                logger.error("Permission problems " + ex);
                success = false;
            }
            setCurrFilePath(pReplicaFilePath);
        }
        else if (destination.equals(KVConstants.SREPLICA)) {
            // DELETE THE current sReplica file
            try {
                Files.deleteIfExists(Paths.get(sReplicaFilePath));
            } catch (IOException ex) {
                // File permission problems are caught here.
                logger.error("Permission problems " + ex);
                success = false;
            }
            setCurrFilePath(sReplicaFilePath);
        }
        else if (destination.equals(KVConstants.COORDINATOR)) {
            setCurrFilePath(serverFilePath);
        }
        else {
            logger.error("MOVE_KVPAIRS destination is not COORDINATOR/PREPLICA/SREPLICA!");
            success = false;
        }
        System.out.println("handleMoveKVPairs writing to file " + currFilePath);
        logger.debug("KVPairs: " + KVPairs);
        String[] kvPairs = KVPairs.split(KVConstants.NEWLINE_DELIM);

        for(int i = 0; i < kvPairs.length ; ++i) {
            String[] kvpair = kvPairs[i].split(KVConstants.SPLIT_DELIM);
            logger.debug("MOVING KVPAIR " + kvpair[0] + " " + kvpair[1]);
            try {
                // POTENTIAL ISSUE! May not want to add KV pair to cache
                putKV(kvpair[0], kvpair[1]);
            } catch (Exception e) {
                logger.error("failed to move KVpair (" + kvpair[0] + ", " + kvpair[1] + ") to server " + getHostname());
                success = false;
            }
        }
        setCurrFilePath(serverFilePath);
        return success;
    }

    private boolean appendToFile (String fileName, String key, String value) {
        BufferedWriter wr  = null;
        boolean success = true;
        try {
            File file = new File(fileName);

            FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
            FileLock lock = null;
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException e) {
                 logger.error("Overlapping File Lock Error: " + e.getMessage());
            }

            if (lock != null) {
                if (!file.exists()) {
                    file.createNewFile();
                }

                FileWriter fw = new FileWriter(file, true);
                wr = new BufferedWriter(fw);
                String KVPair = key + "|" + value + "\n";
                wr.write(KVPair);
                wr.close();
                lock.release();
            }
            else {
                System.out.println("Did not acquire file lock!");
                success = false;
            }
            channel.close();
        }
        catch (IOException io) {
            io.printStackTrace();
            success = false;
        }
        return success;
    }

    public boolean handleUpdateKVPair (String destination, String action, String key, String value) {
        boolean success = true;
        if (destination.equals(KVConstants.PREPLICA)) {
            setCurrFilePath(pReplicaFilePath);
        }
        else if (destination.equals(KVConstants.SREPLICA)) {
            setCurrFilePath(sReplicaFilePath);
        }
        else {
            logger.error("UPDATE destination should always be PREPLICA or SREPLICA!");
            return false;
        }
        System.out.println("handleUpdateKVPair writing to file : " + currFilePath);

        if (action.equals(ReplicaDataAction.NEW.name())) {
            // It is a new KV pair - Append to the end of replica file
            appendToFile(currFilePath, key, value);
        }
        else if (action.equals(ReplicaDataAction.UPDATE.name()) || action.equals(ReplicaDataAction.DELETE.name())) {
            boolean delete = (action.equals(ReplicaDataAction.DELETE.name())) ? true : false;
            writeNewFile(key, value, delete);
        }
        setCurrFilePath(serverFilePath);
        return success;
    }

    private void sendToReplicas(String key, String value, ReplicaDataAction action) {
        logger.debug("Sending key: " + key + " value: " + value + " Action: " + action.name());

        boolean unlock = !isStopped(), success = true;
        lockWrite();

        TextMessage reply;
        // Send to primary & secondary replica
        if (primaryReplica != null) {
            KVStore pReceiver = new KVStore(primaryReplica.getServerAddr(), primaryReplica.getServerPort());
            StringBuilder toSend = new StringBuilder();
            toSend.append("UPDATE" + KVConstants.DELIM);
            try {
                toSend.append(KVConstants.PREPLICA + KVConstants.DELIM);
                toSend.append(action.name() + KVConstants.DELIM);
                toSend.append(key + KVConstants.DELIM + value);
                logger.debug("Command is: " + toSend.toString());
                pReceiver.connect();
                pReceiver.sendMessage(new TextMessage(toSend.toString()));
                reply = pReceiver.receiveMessage();
                pReceiver.disconnect();
                success = reply.getMsg().equals("UPDATE_SUCCESS");
            }
            catch (UnknownHostException e) {
                logger.error("UnknownHostException at sendToReplicas Primary " + e);
                success = false;
            } catch (IOException e) {
                logger.error("IOException at sendToReplicas Primary " + e);
                success = false;
            }
        }

        if (secondaryReplica != null) {
            KVStore sReceiver = new KVStore(secondaryReplica.getServerAddr(), secondaryReplica.getServerPort());
            StringBuilder toSend = new StringBuilder();
            toSend.append("UPDATE" + KVConstants.DELIM);
            try {
                toSend.append(KVConstants.SREPLICA + KVConstants.DELIM);
                toSend.append(action.name() + KVConstants.DELIM);
                toSend.append(key + KVConstants.DELIM + value);
                logger.debug("Command is: " + toSend.toString());
                sReceiver.connect();
                sReceiver.sendMessage(new TextMessage(toSend.toString()));
                reply = sReceiver.receiveMessage();
                sReceiver.disconnect();
                success = success & reply.getMsg().equals("UPDATE_SUCCESS");
            }
            catch (UnknownHostException e) {
                logger.error("UnknownHostException at sendToReplicas Secondary " + e);
                success = false;
            } catch (IOException e) {
                logger.error("IOException at sendToReplicas Secondary " + e);
                success = false;
            }
        }

        if (unlock) {
            unlockWrite();
        }
        logger.info("Result of sendUpdateToReplicas: " + success);
    }

    public void storeKV(String key, String value) throws IOException {
        String filePath  = this.currFilePath;
        BufferedWriter wr  = null;
        boolean toBeDeleted = false; 

        String curVal = onDisk(key);
        if(value == "") {
            toBeDeleted = true;
        }

        if(!curVal.equals("")) {
            //rewrite entire file back with new values
            System.out.println("storeKV: rewriting file toBeDeleted: " + toBeDeleted);
            writeNewFile(key, value, toBeDeleted);
            ReplicaDataAction action = toBeDeleted ? ReplicaDataAction.DELETE : ReplicaDataAction.UPDATE;
            sendToReplicas(key, value, action);
        }
        else {
            if(!toBeDeleted) {
                System.out.println("storeKV: appending to file");
                appendToFile(filePath, key, value);
                sendToReplicas(key, value, ReplicaDataAction.NEW);
            }
        }
    }


    public void writeNewFile(String key, String value, boolean toDelete) {

        String key_val;
        String KVPair;
        String filePath  = this.currFilePath; 
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
                 //logger.error("Overlapping File Lock Error: " + e.getMessage());
            }
            
            if(!file.exists()) {
                logger.error("File not found");
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
                logger.error("Unable to open file. ERROR: " + ex);
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
        logger.debug("DEBUG: key " + encodedKey + " bHash " + metadata.bHash + " eHash " + metadata.eHash);
        logger.debug("DEBUG: minHash " + KVConstants.MIN_HASH + " maxHash " + KVConstants.MAX_HASH);
        boolean ret = false, ret2 = false, ret3 = false;
        if( (metadata.bHash).compareTo(metadata.eHash) < 0 ) {
            ret = (encodedKey.compareTo(metadata.bHash) >= 0  && encodedKey.compareTo(metadata.eHash) < 0);
            logger.debug("DEBUG: isResp? " + ret);
            return ret;
        } else {
            ret2 = (encodedKey.compareTo(metadata.bHash) >= 0 && encodedKey.compareTo(KVConstants.MAX_HASH) < 0);
            ret3 = (encodedKey.compareTo(KVConstants.MIN_HASH) >= 0 && encodedKey.compareTo(metadata.eHash) < 0);
            logger.debug("DEBUG: isResp? " + ret2);
            logger.debug("DEBUG: isResp? " + ret3);
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
            logger.error("Could not find meta data of server " + getHostname() + " " + getPort());
            return false;
        }
        metadata = new ServerMetaData(meta);
        logger.info("Set KVServer (" + metadata.getServerName() + ", " + metadata.getServerAddr() + ", " + metadata.getServerPort() + ") " +
                    "\nStart hash to: " + metadata.getBeginHash().toString(16) + "\nEnd hash to: " + metadata.getEndHash().toString(16));
        return true;
    }

    private void updateTimeStamp() {
        //Update timestamp on the server's Znode
        try {
            String data = zkImplServer.readData(this.zkPath);
            String[] info = data.split(KVConstants.SPLIT_DELIM);
            zkImplServer.updateData(this.zkPath, getZnodeData(info[0], getCurrentTimeString()));
        } catch (KeeperException e) {
           logger.error("ERROR: Unable to update ZK " + e);
        } catch (InterruptedException e) {
           logger.error("ERROR: ZK Interrupted" + e);
        }
    }

    private String getCurrentTimeString() {
        String ret = Long.toString(System.currentTimeMillis());
        //System.out.println("updating kvserver's timestamp.... " + ret);
        return ret;
    }


    public boolean updateReplicas(String pReplicaName, String sReplicaName) {
        boolean success = true;
        logger.debug("Inside updateReplica");
        if (!pReplicaName.equals(KVConstants.NULL_STRING)) {
            String pMetaData = getMetaDataOfServer(pReplicaName);
            if(pMetaData == null) {
                logger.error("Could not find meta data of server " + pReplicaName);
                return false;
            }

            this.primaryReplica = new ServerMetaData(pMetaData);
            String[] pHash = {primaryReplica.getBeginHash().toString(16),  primaryReplica.getEndHash().toString(16)};
            try{
                this.pReplica = true;
                success = moveData(pHash, pReplicaName);
                this.pReplica = false;
            } catch (Exception o) {
                logger.error("MoveData failed for primaryReplica");
            }
        }
        else {
            this.primaryReplica = null;
        }

        if (!sReplicaName.equals(KVConstants.NULL_STRING)) {
            String sMetaData = getMetaDataOfServer(sReplicaName);
            if(sMetaData == null) {
                logger.error("Could not find meta data of server " + sReplicaName);
                return false;
            }
            this.secondaryReplica = new ServerMetaData(sMetaData);
            String[] sHash = {secondaryReplica.getBeginHash().toString(), KVConstants.DELIM, secondaryReplica.getEndHash().toString()};            
            try {
                this.sReplica = true;
                success = moveData(sHash, sReplicaName);
                this.sReplica = false;
            } catch (Exception o) {
                logger.error("MoveData failed for primaryReplica");
            }
        }
        else {
            this.secondaryReplica = null;
        }

        //Connect with each replica and send it all the data you have
        return success;
    }

    @Override
    public void start() {
        writeLocked = false;
        readLocked = false;
        try {
            zkImplServer.updateData(this.zkPath, getZnodeData("SERVER_STARTED", getCurrentTimeString()));
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
        try {
            zkImplServer.updateData(this.zkPath, getZnodeData("SERVER_STOPPED", getCurrentTimeString()));
        } catch (KeeperException e) {
            logger.error("ERROR: Unable to update ZK " + e);
        } catch (InterruptedException e) {
            logger.error("ERROR: ZK Interrupted" + e);
        } catch (Exception e) {
            logger.error("ERROR: ZK Exception" + e);
        }
        this.timeStamper.stop();
    }

    @Override
    public void shutdown() {
        writeLocked = true;
        readLocked = true;
        running = false;
        try {
            zkImplServer.updateData(this.zkPath, getZnodeData("SERVER_SHUTDOWN", getCurrentTimeString()));
        } catch (KeeperException e) {
            logger.error("ERROR: Unable to update ZK " + e);
        } catch (InterruptedException e) {
            logger.error("ERROR: ZK Interrupted" + e);
        }
        this.timeStamper.stop();
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
        logger.debug("DEBUG: getHostname() = " + getHostname());
        logger.debug("DEBUG: targetName() = " + targetName);
        if(targetName.equals(getHostname())) return true;
        boolean unlock = !this.isStopped();
        lockWrite();
        StringBuilder toSend = new StringBuilder();
        toSend.append("MOVE_KVPAIRS" + KVConstants.DELIM);
        if(pReplica) {
            logger.debug("Writing to pReplica : " + targetName);
            toSend.append(KVConstants.PREPLICA + KVConstants.DELIM);
        }
        else if(sReplica) {
            logger.debug("Writing to sReplica: " + targetName);
            toSend.append(KVConstants.SREPLICA + KVConstants.DELIM);
        }
        else {
            logger.debug("Writing to Coordinator: " + targetName);
            toSend.append(KVConstants.COORDINATOR + KVConstants.DELIM);
        }
        logger.debug("Command is: " + toSend.toString());
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
                    String[] kvp = line.split(KVConstants.SPLIT_DELIM);
                    if(this.moveAll || !isResponsible(kvp[0])) {
                        logger.debug(getHostname() + " not responsible for key " + kvp[0]);
                        found = true;
                        toSend.append(line + KVConstants.NEWLINE_DELIM);
                        if(!(pReplica || sReplica)) {
                            toDelete.add(kvp[0]);
                        }
                    }
                    else {
                        logger.debug(getHostname() + " responsible for key " + kvp[0]);
                    }
                }
            }
            else {
                // TODO we don't need to creat a file. putKV will do it on its own
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
                logger.debug("Number of keys to delete: " + toDelete.size());
                //For replicas, toDelete should be empty
                for(String key: toDelete) {
                    deleteKV(key);
                }

            }
        } else {
            logger.debug("No data to move from " + getHostname());
        }
        if(unlock) {
            unlockWrite();
        }
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
