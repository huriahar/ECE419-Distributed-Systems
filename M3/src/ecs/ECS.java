package ecs;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;
import java.util.HashMap;
import java.math.BigInteger;
import java.net.Socket;
import java.util.Iterator;
import java.io.IOException;
import java.lang.InterruptedException;
import java.net.SocketTimeoutException;
import java.net.SocketException;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.File;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.Collection;

import org.apache.log4j.Logger;
import java.lang.Process;

import org.apache.zookeeper.KeeperException;

import common.*;
import common.messages.TextMessage;

import java.util.concurrent.TimeUnit;

public class ECS implements IECS {
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    public static final String metaFile = "metaDataECS.config";            
    public static final String lastRemovedFile = "lastRemoved.txt";    
    private Path configFile;
    private Path metaDataFile;
    private Socket ECSSocket;

    private TreeMap<BigInteger, IECSNode> ringNetwork;
    // IECSNode and status {"Available", "Taken"} - TODO: Convert to an ENUM
    private HashMap<IECSNode, String> allAvailableServers;
    private static Logger logger = Logger.getRootLogger();
    private OutputStream output; 
    private InputStream input;
    private String lastRemovedName = null;
    private String ugmachine;
    //For the autotester
    private String zkHostname;
    private int zkPort;    
    
    private ZKImplementation ZKImpl;

    public ECS(Path configFile, String ugmachine) {
        //First, start zookeeper
        try {
            String cmd = System.getProperty("user.dir") + "/zookeeper-3.4.11/bin/zkServer.sh start";
            logger.info("Starting zookeeper: " + cmd); 
            Process p = Runtime.getRuntime().exec(new String[]{"csh","-c", cmd});
        } catch (IOException e) {
            logger.error("could not start zookeeper: " + e); 
        }
        this.ugmachine = ugmachine;
        this.configFile = configFile;
        this.ringNetwork = new TreeMap<BigInteger, IECSNode>();
        this.allAvailableServers = new HashMap<IECSNode, String>();
        this.ZKImpl = new ZKImplementation();
        try {
        	if (Files.exists(Paths.get(metaFile))) {
        		Files.delete(Paths.get(metaFile));
        	}
            this.metaDataFile = Files.createFile(Paths.get(metaFile));
            populateAvailableNodes();
            ZKImpl.zkConnect(ugmachine);
            ZKImpl.deleteGroup(KVConstants.ZK_ROOT);
            ZKImpl.createGroup(KVConstants.ZK_ROOT);
        }
        catch (IOException e) {
            logger.error("Unable to open metaDataFile " + e);
        }
        catch (InterruptedException e) {
        	logger.error("Unable to connect to Zookeeper " + e);
		}
        catch (KeeperException e) {
			logger.error("Unable to create group in Zookeeper " + e);
		}     
    }
    
    public void setZKInfo(String zkHostname, int zkPort) {
        this.zkHostname = zkHostname;
        this.zkPort = zkPort;
    }

    private void populateAvailableNodes() {
        try {
            ArrayList<String> lines = new ArrayList<>(Files.readAllLines(this.configFile, StandardCharsets.UTF_8));
            int numServers = lines.size();
            
            for (int i = 0; i < numServers ; ++i) {
                IECSNode node = new ECSNode(lines.get(i), KVConstants.CONFIG_DELIM);
                // TODO: Change this to an ENUM
                String status = "AVAILABLE";
                allAvailableServers.put(node, status);
            }    
        } catch (IOException io) {
            logger.error("Unable to open config file");
        }
    }

    private boolean inRingNetwork(IECSNode node) {
        String name = node.getNodeName();
        boolean found = false;
        for(Map.Entry<BigInteger, IECSNode> entry: ringNetwork.entrySet()) {
             
            if (entry.getValue().getNodeName().equals(name)) {
                found = true;
                break;
            }
        }
        return found;
    }

    public int ringNetworkSize() {
        return this.ringNetwork.size();
    }

    public int availableServersCount() {
        int count = 0;
        for(Map.Entry<IECSNode, String> entry : allAvailableServers.entrySet()) {
            if(entry.getValue().equals("AVAILABLE")) {
                count++;
            }
        }
        return count;
    }

    public int availableServers() {
        return allAvailableServers.size();
    }

    public boolean sendWriteUnlock(IECSNode node) {
        boolean success = true;
        TextMessage message, response; 
        try { 
            message = new TextMessage("ECS" + KVConstants.DELIM + "WRITE_UNLOCK");
            response = sendNodeMessage(message, node);
            success = response.getMsg().equals("UNLOCK_SUCCESS");
            if(success) {
                logger.info("Unlocked KVServer: " + node.getNodeName() + " <" + node.getNodeHost() + "> <" + 
                node.getNodePort() + ">"); 
            } else {
                logger.error("WRITE_UNLOCK ERROR for KVServer: " + node.getNodeName());
            }
        } catch(IOException io) {
            logger.error("WRITE_UNLOCK ERROR for KVServer: " + node.getNodeName() + ". Error: " + io);
            success = false;
        } 
        return success;
    }
    
    public boolean sendWriteLock(IECSNode node) {
        boolean success = true;
        TextMessage message, response; 
        try { 
            message = new TextMessage("ECS" + KVConstants.DELIM + "WRITE_LOCK");
            response = sendNodeMessage(message, node);
            success = response.getMsg().equals("LOCK_SUCCESS");
            if(success) {
                logger.info("Locked KVServer: " + node.getNodeName() + " <" + node.getNodeHost() + "> <" + 
                node.getNodePort() + ">"); 
            } else {
                logger.error("WRITE_LOCK ERROR for KVServer: " + node.getNodeName());
            }
        } catch(IOException io) {
            logger.error("WRITE_LOCK ERROR for KVServer: " + node.getNodeName() + ". Error: " + io);
            success = false;
        } 
        return success;
    }
    
    public boolean start(IECSNode node) {
        boolean success = true;
        TextMessage message, response; 
            
        try { 
            message = new TextMessage("ECS" + KVConstants.DELIM + "START_NODE");
            response = sendNodeMessage(message, node);
            success = response.getMsg().equals("START_SUCCESS");
            if(success) {
                logger.info("Started KVServer: " + node.getNodeName() + " <" + node.getNodeHost() + "> <" + 
                node.getNodePort() + ">"); 
            } else {
                logger.error("START ERROR for KVServer: " + node.getNodeName());
                success = false;
            }
        } catch(IOException io) {
            logger.error("START ERROR for KVServer: " + node.getNodeName() + ". Error: " + io);
            success = false;
        } 
        return success;
    }

        
    public boolean stop(IECSNode node) {
        boolean success= true;
        TextMessage message, response; 
        
        try { 
            message = new TextMessage("ECS"  + KVConstants.DELIM + "STOP_NODE");
            response = sendNodeMessage(message, node);
            if(response.getMsg().equals("STOP_SUCCESS")) 
                logger.info("Stop KVServer: " + node.getNodeName() + " <" + node.getNodeHost() + "> <" + 
                node.getNodePort() + ">"); 
            else if(response.getMsg().equals("STOP_FAILURE")) { 
                logger.error("STOP ERROR for KVServer: " + node.getNodeName());
                success = false;
            }

        } catch(IOException ex) {
            logger.error("STOP ERROR for KVServer: " + node.getNodeName() + ". Error: " +  ex);
            success = false;
        } 
        return success;
    }


    public void deleteMetaDataFile() throws IOException {
       Files.deleteIfExists(metaDataFile); 

    }

    public void updateMetaDataFile() throws IOException {

        ArrayList<String> metaDataContent = new ArrayList<String>();
        IECSNode node;
        for(Map.Entry<BigInteger, IECSNode> entry : ringNetwork.entrySet()) {
            // Prepare writable content to write to metaDatafile    
            node = entry.getValue();
            metaDataContent.add(node.getNodeName() + KVConstants.DELIM +
                            node.getNodeHost() + KVConstants.DELIM +
                            node.getNodePort() + KVConstants.DELIM +
                            node.getNodeHashRange()[0].toString(16) + KVConstants.DELIM +
                            node.getNodeHashRange()[1].toString(16));      

        }
        printRing();
        Files.write(metaDataFile, metaDataContent, StandardCharsets.UTF_8);
        return;
    }

    public void printRing() {
        IECSNode node;        
        for(Map.Entry<BigInteger, IECSNode> entry : ringNetwork.entrySet()) {
            node = entry.getValue();
            logger.debug(node.getNodeName() + " : " + node.getNodeHashRange()[0] + " : " + node.getNodeHashRange()[1]);
            
        }               
    } 
   
    public boolean sendMetaDataUpdate(IECSNode rNode) {
        boolean success = true;
        TextMessage response, message;
        // Step 1
        // Ask the rNode to update it's local metadata range
        // TODO needs no target
        message = new TextMessage("ECS" + KVConstants.DELIM + "UPDATE_METADATA");
        try {
            response = sendNodeMessage(message, rNode);
            if(response.getMsg().equals("UPDATE_SUCCESS")) {
                logger.info("SUCCESS: MetaData updated for KVServer: " + rNode.getNodeName());
            }
            else if(response.getMsg().equals("UPDATE_FAILED")) {
                logger.error("ERROR: MetaData not updated for KVServer: " + rNode.getNodeName());
                success = false;
            } else {
                logger.error("ERROR: MetaData not updated for KVServer: " + rNode.getNodeName());
                success = false;
            }
        } catch (IOException ex) {
            success = false;
            logger.error("UPDATE_ERROR: Update for KVServer failed: " + ex);
        }
        return success;
    }

    public boolean sendMoveKVPairs(IECSNode srcNode, IECSNode dstNode, boolean moveAll) {
        boolean success = true;
        TextMessage response, message;
        String cmd = moveAll ? "MOVE_ALL_KVPAIRS" : "MOVE_KVPAIRS";
        // Step 2
        // Ask the removed node to move all its KVPairs to its successor
        // The msg format is MOVE_ALL_KVPAIRS|targetName|targetHashBegin|targetHashEnd
        message = new TextMessage("ECS" + KVConstants.DELIM +
                                cmd + KVConstants.DELIM +
                                dstNode.getNodeName() + KVConstants.DELIM +
                                dstNode.getNodeHashRange()[0].toString(16) + KVConstants.DELIM +
                                dstNode.getNodeHashRange()[1].toString(16));
        try {
            response = sendNodeMessage(message, srcNode);
            if(response.getMsg().equals("MOVE_SUCCESS")) {
                logger.info("SUCCESS: MetaData updated for KVServer: " + srcNode.getNodeName());
            }
            else if(response.getMsg().equals("MOVE_FAILED")) {
                logger.error("ERROR: MetaData not moved for KVServer: " + srcNode.getNodeName());
                success = false;
            } else {
                logger.error("ERROR: MetaData not moved for KVServer: " + srcNode.getNodeName());
                success = false;
            }
        } catch (IOException ex) {
            success = false;
            logger.error("UPDATE_ERROR: Update for KVServer failed: " + ex);
        }
        return success;
    }

    public IECSNode findAvailableServer() {
        for(Map.Entry<IECSNode, String> entry : allAvailableServers.entrySet()) {
            if(entry.getValue().equals("AVAILABLE")) {
                //Add the server to the ringNetwork and update HashMap 
                entry.setValue("TAKEN");
                IECSNode currNode = entry.getKey();
                return currNode;
            }
        }
        return null;
    }
 
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        //Select node from available, update hashing, add to hashRing and alert all servers to upate metaData 
        IECSNode currNode = new ECSNode(), nextNode = new ECSNode();     
        BigInteger serverHash = new BigInteger("0", 16);
        boolean success = true;
        try { 
            // Find an available server
            currNode = findAvailableServer();
            // Launch the server
            if(launchKVServer(currNode, cacheStrategy, cacheSize)) {
                logger.info("SUCCESS. Launched KVServer :" + currNode.getNodeName());
            } 
            else {
                logger.error("ERROR. Unable to launch KVServer :" + currNode.getNodeName() + " Host: " + currNode.getNodeHost()+ " Port: " + currNode.getNodePort());           
                return null;
            }
            // Update hashes of current and next server on the ring network
            serverHash = md5.encode(currNode.getNodeHost() + KVConstants.HASH_DELIM + currNode.getNodePort());
            currNode = updateHash(serverHash, currNode);
            // Now update the metadata file, write to metaDataFile and alert nodes to update their metaData
            updateMetaDataFile();
            // Update meta data for the node AFTER the added node
            // Move KVpairs from nextNode to addedNode
            nextNode = findNextNode(serverHash);
            if(nextNode == null) {
                logger.error("Could not find next node while adding a new node!!");
                return null;
            }
            // Send metadata update and setup cache config
            success = sendMetaDataUpdate(currNode);
            success = success & setupNodesCacheConfigOneNode(currNode, cacheStrategy, cacheSize);
            if(nextNode != currNode) {
                success = success & sendMetaDataUpdate(nextNode);
                success = success & sendMoveKVPairs(nextNode, currNode, false);
            } else {
                logger.debug("nextNode is the same as currNode");
            }
            if(!success) {
                logger.error("in addNode: unable to update metaData in Servers");
            }

        }
        catch (IOException io) {
            logger.error("Unable to write to metaDataFile"); 
        }
        //Other errors ??
        return currNode; 
    }

    private String getLastRemoved() {
        try {
            File f = new File(this.lastRemovedFile);
            if(f.exists()) {
                ArrayList<String> lines = new ArrayList<>(Files.readAllLines(f.toPath(), StandardCharsets.UTF_8));
                return lines.get(0);
            }
        } catch (IOException io) {
            logger.error("Unable to open config file: " + io);
        }
        return null;
    }

    //This is a helper function for initAddNodesToHashRing
    private boolean isServerSuitable(String nodeName, String status) {
        boolean success = true;
        // if lastRemovedName is null, we couldn't find a lastRemovedFile or couldn't
        // open it, in which case we want to accept the first available server
        success = (lastRemovedName == null || nodeName.equals(lastRemovedName));
        return success & status.equals("AVAILABLE");
    }

    public IECSNode initAddNodesToHashRing() {
        //This function is ONLY used for the first node added to the system/ringNetwork
        IECSNode chosenNode = null;     
        int counter = 0, numNodes = 1;
        lastRemovedName = getLastRemoved();
        if(lastRemovedName != null){
            lastRemovedName = lastRemovedName.split("\\" + KVConstants.DELIM)[0];
        }
    
        try { 
            // Initialize own hashRing
            for(Map.Entry<IECSNode, String> entry : allAvailableServers.entrySet()) {
                if(counter < numNodes) {
                    String nodeName = entry.getKey().getNodeName();
                    String status = entry.getValue();
                    if(isServerSuitable(nodeName, status)) {
                        logger.debug("found an avalable server " + entry.getKey().getNodeName());
                        counter++;
                        entry.setValue("TAKEN");
                        IECSNode node = entry.getKey();
                        BigInteger serverHash = md5.encode(node.getNodeHost() +
                                                    KVConstants.HASH_DELIM +
                                                    Integer.toString(node.getNodePort()));
                        //Setup begin and end hashing for server 
                        node = updateHash(serverHash, node);
                        //Add chosen node to collection
                        chosenNode = node;
                        break;
                    }
                }
            }
             
            //Once all nodes are added, write to metaDataFile
            updateMetaDataFile();   
            //the alert will be called once launching of the servers is done from ECSClient
        } catch (IOException io) {
            logger.error("Unable to write to metaDataFile");
        }
        
        return chosenNode; 
    }

    public boolean setupNodesCacheConfigOneNode(IECSNode node, String cacheStrategy, int cacheSize) {
        TextMessage message = new TextMessage("ECS" + KVConstants.DELIM + "SETUP_NODE" + KVConstants.DELIM + cacheStrategy + KVConstants.DELIM + cacheSize); 
        TextMessage response;
        boolean success = true;
        try {
            response = sendNodeMessage(message, node);
            if(response.getMsg().equals("SETUP_SUCCESS")) {
                logger.info("SUCCESS: Setup for KVServer: " + node.getNodeName()); 
            }
            else if(response.getMsg().equals("SETUP_FAILED")) {
                logger.error("ERROR: Failed in Setup for KVServer: " + node.getNodeName());
                success = false;
            } else {
                logger.error("ERROR: Failed in Setup for KVServer: " + node.getNodeName());
                success = false;
            }
        } catch(IOException ex) {
            logger.error("ERROR: Failed in Setup for KVServer: " + node.getNodeName()); 
            success = false;
        } 
        return success;
    }

    public Collection<IECSNode> setupNodesCacheConfig(Collection<IECSNode> nodes, String cacheStrategy, int cacheSize) {
        Collection<IECSNode> nodesResult = new ArrayList<IECSNode>();
        TextMessage message = new TextMessage("ECS" + KVConstants.DELIM + "SETUP_NODE" + KVConstants.DELIM + cacheStrategy + KVConstants.DELIM + cacheSize); 
        TextMessage response;
        for(IECSNode node : nodes) {
        	try {
        		response = sendNodeMessage(message, node);
        		if(response.getMsg().equals("SETUP_SUCCESS")) {
                    logger.info("SUCCESS: Setup for KVServer: " + node.getNodeName()); 
                    nodesResult.add(node); 
                }
                else if(response.getMsg().equals("SETUP_FAILED")) {
                    logger.error("ERROR: Failed in Setup for KVServer: " + node.getNodeName());
                } else {
                    logger.error("ERROR: Failed in Setup for KVServer: " + node.getNodeName());
                }
            } catch(IOException ex) {
                logger.error("ERROR: Failed in Setup for KVServer: " + node.getNodeName()); 
            } 
        }
        return nodesResult;
    }

    public IECSNode updateHash(BigInteger nodeHash, IECSNode currNode) {
    	
    	assert currNode != null;
        
        IECSNode prevNode = new ECSNode(), nextNode = new ECSNode();

        // Only one in network, so start and end are yours
        if(ringNetwork.isEmpty()) {
            currNode.setNodeBeginHash(nodeHash);
            currNode.setNodeEndHash(nodeHash);
            ringNetwork.put(currNode.getNodeHashRange()[1], currNode);
            return currNode;
        }
        else if(ringNetwork.containsKey(nodeHash)) {
            logger.error("ERROR KVServer already in ringNetwork");
            return null;
        }
        nextNode = findNextNode(nodeHash);
        if(nextNode == null) {
            logger.error("nextNode is null wth");
            return null;
        }
        currNode.setNodeBeginHash(nextNode.getNodeHashRange()[0]);
        currNode.setNodeEndHash(nodeHash);
        nextNode.setNodeBeginHash(nodeHash);
        //update ringNetwork
        ringNetwork.put(nextNode.getNodeHashRange()[1], nextNode);
        ringNetwork.put(currNode.getNodeHashRange()[1], currNode);
        return currNode; 
    }

    public String getZKPath(String nodeName) {
        return  KVConstants.ZK_SEP + KVConstants.ZK_ROOT + KVConstants.ZK_SEP + nodeName;
    }

    public String checkZKStatus(String path, long timeout) {
        //Check status of ZK
        String data = null;
        long currTime = System.currentTimeMillis();
        try {
            data = ZKImpl.readData(path);			
    	} catch (KeeperException e) {
            logger.error("ERROR: Keeper Exception for ZK: " + e);
    	} catch (InterruptedException e) {
            logger.error("ERROR: Interrupted Exception for ZK: " + e);
        }
        String[] zNodeData = data.split("\\" + KVConstants.DELIM);
        while(currTime > timeout || !zNodeData[0].equals("SERVER_LAUNCHED")) {
            currTime = System.currentTimeMillis();
    	    zNodeData = data.split("\\" + KVConstants.DELIM);
        }
        return zNodeData[0];
    }

    public boolean launchKVServer(IECSNode node, String cacheStrategy, int cacheSize) {
        boolean success = true;
        Process proc;
        logger.info("Launching <" + node.getNodeHost() + ", " + node.getNodePort() + ">");

        Runtime run = Runtime.getRuntime();
        try {
            String[] launchCmd = {"m2ssh.sh", ugmachine, System.getProperty("user.dir"), node.getNodeName(), node.getNodeHost(), Integer.toString(node.getNodePort())};
            proc = run.exec(launchCmd);
            // Also create a new znode for the node
            // Update data on znode about server config and join ZK_ROOT group
            ZKImpl.joinGroup(KVConstants.ZK_ROOT, node.getNodeName());
            String data = "SERVER_STOPPED" + KVConstants.DELIM + node.getNodeHost() + KVConstants.DELIM + node.getNodePort();
            String path = KVConstants.ZK_SEP + KVConstants.ZK_ROOT + KVConstants.ZK_SEP + node.getNodeName();
            ZKImpl.updateData(path, data);
            Thread.sleep(KVConstants.LAUNCH_TIMEOUT);

        } catch (InterruptedException ex) {
            logger.error("ERROR: KVServer Thread unable to sleep");
            success = false;
        } catch (IOException io) {
            logger.error("ERROR: KVServer IOException: " + io);
            success = false;
        } catch (KeeperException e) {
        	logger.error("ERROR: Unable to add node to ZK " + e);
            success = false;
        }
        if(success) {
            logger.info("KVServer process launched successfully");
        }
        return success;
    }


    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    public void printDebug(String dbgmsg) {
        logger.debug("DEBUG! " + dbgmsg);
    }

    public boolean removeNodes(Collection<IECSNode> nodes) {
        if(nodes.size() == 0) return true;
        //Need to let the node know to stop
    	BigInteger currNodeHash;    
        IECSNode nextNode = null, currNode = null, node;
        boolean onlyOneNode = false;
        boolean success = true;
        for(IECSNode server: nodes) {
            Iterator<Map.Entry<BigInteger, IECSNode>> iter = ringNetwork.entrySet().iterator();
            while( iter.hasNext()) {
                Map.Entry<BigInteger, IECSNode> entry = iter.next();
                if(server.getNodeName().equals(entry.getValue().getNodeName())) {

                    //Found the node we want to remove
                    currNodeHash = entry.getKey();
                    currNode = entry.getValue();
                    nextNode = findNextNode(currNodeHash);
                    if(nextNode == null) {
                        logger.error("ERROR: There should be at least one node in the network!! found none!");
                        return false;
                    }
                    onlyOneNode = (nextNode.getNodeHashRange()[1] == currNodeHash);
                    nextNode.setNodeBeginHash(currNode.getNodeHashRange()[0]);
                    try {
                        //Need to remove node from network before updating all the metadata
                        iter.remove();
                        //Update the changed hashRange to the ring
                        if(!onlyOneNode) {
                            ringNetwork.put(nextNode.getNodeHashRange()[1], nextNode);
                        }
                        else {
                            //TODO keep track of the name of the last node dying so we can start
                            // with it initially
                            //TODO whenever the ring is empty start up the node whose name is in the file
                            try {
                                String lastNode = currNode.getNodeName() + KVConstants.DELIM +
                                                  currNode.getNodeHost() + KVConstants.DELIM +
                                                  currNode.getNodePort();
                                Path file = Paths.get(this.lastRemovedFile);
                                Files.write(file, lastNode.getBytes());
                            } catch (Exception e) {
                                logger.error("could not write last removed node to lastRemovedFile");
                                logger.error(e);
                            }
                        }

                        updateMetaDataFile();
                        // Update meta data for the node AFTER the removed node
                        // Move KVpairs from removedNode to nextNode
                        if(!onlyOneNode) {
                            success = sendMetaDataUpdate(nextNode);
                            success = success & sendMoveKVPairs(currNode, nextNode, true);
                        }
                    
                    } catch (IOException io) {
                        logger.error("ERROR: Unable to update metaData with nodes removed");
                        success = false;
                    }
                    success = success & shutDownOneNode(currNode);
                    success = success & removeZKNode(currNode);
                }
                
            }
        }
        return success;
    }

    public boolean removeZKNode(IECSNode node) {
        try {
            String path = KVConstants.ZK_ROOT + KVConstants.ZK_SEP + node.getNodeName();
            ZKImpl.deleteGroup(path);
        } catch (InterruptedException e) {
        	logger.error("ERROR: Unable to remove znode from ZK for server" + node.getNodeName() + ": " + e);
            return false;
        } catch (KeeperException e) {
        	logger.error("ERROR: Unable to remove znode from ZK for server" + node.getNodeName() + ": " + e);
            return false;
		}
        return true;
    }

    public BigInteger findPrevKey(BigInteger currHash) {
        if(ringNetwork.size() < 1) {
            return new BigInteger("0", 16);
        } else {
            if(ringNetwork.lowerKey(currHash) == null) {
                return ringNetwork.lastKey();
            }
            else {
                return ringNetwork.lowerKey(currHash);
            }
        }
    }

    public IECSNode findNextNode(BigInteger currHash) {
        IECSNode nextNode = new ECSNode();
        //We only want to return null if ring is empty or node not in ring
        if(ringNetwork.size() < 1) {
            nextNode = null;
        } else {
            if(ringNetwork.higherKey(currHash) == null) {
                nextNode = ringNetwork.firstEntry().getValue();
            }
            else {
                nextNode = ringNetwork.higherEntry(currHash).getValue();
            }
        }
        return nextNode; 
    }
    
    // Return a map of all nodes.
    // Server Name -> IECSNode
    public Map<String, IECSNode> getNodes() {
        Map<String, IECSNode> map = new HashMap<String, IECSNode>();
        for(Map.Entry<BigInteger, IECSNode> entry: ringNetwork.entrySet()) {
            map.put(entry.getValue().getNodeName(), entry.getValue());
        }
        return map;
    }

    public IECSNode getNodeByKey(String Key) {
        IECSNode serverNode;
        if(ringNetwork.isEmpty()) return null;
        BigInteger encodedKey = md5.encode(Key);
        if(ringNetwork.higherEntry(encodedKey) == null) {
            serverNode = ringNetwork.firstEntry().getValue();
        }
        serverNode = ringNetwork.higherEntry(encodedKey).getValue();
        return serverNode;
    }

    public TextMessage sendNodeMessage(TextMessage message, IECSNode node)
    			throws IOException {
        TextMessage response = new TextMessage(""); 
        try {
            connectNode(node);
        } catch (IOException e) {
            try{
                TimeUnit.SECONDS.sleep(4);
                connectNode(node);
            } catch (IOException ex) {
                logger.error("Failed to connect to server <" + node.getNodeHost() + ":" + node.getNodePort() + ">. KVServer launch script may have failed.");
                logger.error(ex);
                return response;
            } catch (InterruptedException ex) {
                logger.error("Failed to connect to server <" + node.getNodeHost() + ":" + node.getNodePort() + ">. KVServer launch script may have failed.");
                logger.error(ex);
                return response;
            }
        }
        sendMessage(message);
        //Receives a connection confirmation message is right
        try {
            response = receiveMessage();
            //Receive the response regarding the message sent
            response = receiveMessage();
        } catch (SocketException e) {
            logger.error("Timeout while waiting for server's response! " + e);
        }
        disconnect();
        return response;
    }

    public void connectNode(IECSNode server) 
    		throws IOException {
        this.ECSSocket = new Socket(server.getNodeHost(), server.getNodePort());
        this.ECSSocket.setSoTimeout(KVConstants.LAUNCH_TIMEOUT);
        logger.info("ECS Connection to name: " + server.getNodeName() + " successful");
    }


    public void disconnect() {
        // TODO: Call each of the servers to close as well
        try {
            if(ECSSocket != null) {
                ECSSocket.close();
                ECSSocket = null;
            }

        } catch(Exception io) {
            logger.error("ERROR: Unable to close ECS socket");
        }
    }

        
    /**
     * Method sends a TextMessage using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream 
     */
    public void sendMessage(TextMessage msg)
            throws IOException {
        output = this.ECSSocket.getOutputStream();
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("SEND \t<" 
                + ECSSocket.getInetAddress().getHostAddress() + ":" 
                + ECSSocket.getPort() + ">: '" 
                + msg.getMsg() +"'");
    }

    private TextMessage receiveMessage()
            throws IOException {
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];
        /* read first char from stream */
        input = this.ECSSocket.getInputStream();
        byte read = (byte) input.read();    

        boolean reading = true;

        while(read != 10 && read != -1 && reading) {/* CR, LF, error */
            /* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            } 
            
            /* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;
            
            /* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }
            
            /* read next char from stream */
            
            read = (byte) input.read();
        }
        
        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        }
        else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }
        
        msgBytes = tmp;

        if (new String(msgBytes).trim().isEmpty()) throw new IOException();
        
        /* build final String */
        TextMessage msg = new TextMessage(msgBytes);
        logger.info("RECEIVE \t<" 
                + ECSSocket.getInetAddress().getHostAddress() + ":" 
                + ECSSocket.getPort() + ">: '" 
                + msg.getMsg().trim() + "'");
        return msg;
    }

    public boolean shutDownOneNode(IECSNode node) {
        try {
            stop(node);
            //send shutdown message
            TextMessage message = new TextMessage("ECS" + KVConstants.DELIM + "SHUTDOWN_NODE");
            TextMessage response;
            //get response
            response = sendNodeMessage(message, node);
            if(response.getMsg().equals("SHUTDOWN_SUCCESS")) 
                logger.info("SUCCESS: Shutdown KVServer: " + node.getNodeName());
            else {
                logger.error("ERROR: Unable to shutdown node");
                return false;
            }
            allAvailableServers.put(node, "AVAILABLE");
        } catch(IOException io) {
            logger.error("ERROR: Unable to shutdown node");
            return false;    
        }
        return true;
    }


    public boolean ECSShutDown() {
        //Shutdown ECS
        boolean success = true;
        if(ringNetwork.isEmpty()) return success;
        for(Map.Entry<BigInteger, IECSNode> entry: ringNetwork.entrySet()) {
            shutDownOneNode(entry.getValue());
        }

        //clear the Hash Ring and the set of available nodes
        ringNetwork.clear();
        allAvailableServers.clear();

        try {
            deleteMetaDataFile();
        } catch (IOException ex) {
            logger.error("ERROR: Unable to delete ECS MetaData File");
            success = false;            
        }
            
        return success;
    }

	@Override
	public boolean shutdown() {
        //clear the Hash Ring and the set of available nodes
        ringNetwork.clear();
        allAvailableServers.clear();
        try {
            deleteMetaDataFile();
        } catch (IOException ex) {
            logger.error("ERROR: Unable to delete ECS MetaData File");
            return false;            
        }
		return true;
	}
}
