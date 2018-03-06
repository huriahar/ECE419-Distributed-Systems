package ecs;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;
import java.util.HashMap;
import java.net.Socket;

import java.io.IOException;
import java.lang.InterruptedException;

import java.io.OutputStream;
import java.io.InputStream;

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

public class ECS implements IECS {
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    private static final int LAUNCH_TIMEOUT = 2000;
    public static final String metaFile = "metaDataECS.config";            
    
    private Path configFile;
    private Path metaDataFile;
    private Socket ECSSocket;

    private TreeMap<String, IECSNode> ringNetwork;
    // IECSNode and status {"Available", "Taken"} - TODO: Convert to an ENUM
    private HashMap<IECSNode, String> allAvailableServers;
    private static Logger logger = Logger.getRootLogger();
    private OutputStream output; 
    private InputStream input;
    
    private ZKImplementation ZKImpl;

    public ECS(Path configFile) {
        this.configFile = configFile;
        this.ringNetwork = new TreeMap<String, IECSNode>();
        this.allAvailableServers = new HashMap<IECSNode, String>();
        this.ZKImpl = new ZKImplementation();
        try {
            if(!Files.exists(Paths.get(metaFile)))
                this.metaDataFile = Files.createFile(Paths.get(metaFile));
            else {
                this.metaDataFile = Paths.get(metaFile);
                populateRingNetwork();
            }
            populateAvailableNodes();
            ZKImpl.zkConnect("localhost");
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
    
    private void populateAvailableNodes() {
        try {
            ArrayList<String> lines = new ArrayList<>(Files.readAllLines(this.configFile, StandardCharsets.UTF_8));
            int numServers = lines.size();
            
            
            for (int i = 0; i < numServers ; ++i) {
            	String[] serverInfo = lines.get(i).split(" ");
            	int serverPort = Integer.parseInt(serverInfo[2]);
                IECSNode node = new ECSNode(serverInfo[0], serverInfo[1], serverPort);
                // TODO: Change this to an ENUM
                String status = "AVAILABLE";
                allAvailableServers.put(node, status);
            }    
        } catch (IOException io) {
            logger.error("Unable to open config file");
        }
    }

    private void populateRingNetwork()
            throws IOException {
        ArrayList<String> lines = new ArrayList<>(Files.readAllLines(this.metaDataFile, StandardCharsets.UTF_8));
        int numServers = lines.size();
        
        for (int i = 0; i < numServers ; ++i) {
        	System.out.println(lines.get(i));
            IECSNode node = new ECSNode(lines.get(i));
            String serverHash = md5.encode(node.getNodeName() + KVConstants.DELIM +
                                           node.getNodeHost() + KVConstants.DELIM +
                                           node.getNodePort());
            System.out.println("serverHash " + serverHash);
            this.ringNetwork.put(serverHash, node);
        }
    }

    public int availableServers() {
        return allAvailableServers.size();
    }

    public boolean start(IECSNode node) {
        boolean success = true;
        TextMessage message, response; 
            
        try { 
            message = new TextMessage("ECS" + KVConstants.DELIM + "START_NODE");
            response = sendNodeMessage(message, node);
            if(response.getMsg().equals("START_SUCCESS")) 
                logger.info("Started KVServer: " + node.getNodeName() + " <" + node.getNodeHost() + "> <" + 
                node.getNodePort() + ">"); 
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


    public boolean shutDownNode(IECSNode node) throws IOException {
        //Send Shutdown to node passed in  
        boolean success = true;  
        TextMessage message = new TextMessage("ECS" + KVConstants.DELIM + "SHUTDOWN_NODE");
        TextMessage response;
        response = sendNodeMessage(message, node);
        if(response.getMsg().equals("SHUTDOWN_SUCCESS")) 
            logger.info("SUCCESS: Shutdown KVServer: " + node.getNodeName());
        return success;
    }

    public boolean ECSShutDown() {
        //Shutdown ECS
        boolean success = true;
        if(ringNetwork.isEmpty()) return success;
        for(Map.Entry<String, IECSNode> entry: ringNetwork.entrySet()) {
            try {
                shutDownNode(entry.getValue());
            } catch(IOException io) {
                logger.error("ERROR: Unable to shutdown KVServer: " + entry.getValue().getNodeName());
                success = false;
            } 
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

    public void deleteMetaDataFile() throws IOException {
       Files.deleteIfExists(metaDataFile); 

    }

    public void updateMetaData(ArrayList<String> data) throws IOException {
        Files.write(metaDataFile,data, StandardCharsets.UTF_8);
        alertMetaDataUpdate();
        return;
    }
       
    public void alertMetaDataUpdate() {
        TextMessage message = new TextMessage("ECS" + KVConstants.DELIM + "UPDATE_METADATA");
        TextMessage response;
        IECSNode node = new ECSNode();
        for(Map.Entry<String, IECSNode> entry: ringNetwork.entrySet()) {
            node = entry.getValue();
            try {
                response = sendNodeMessage(message, node);
                if(response.getMsg().equals("UPDATE_SUCCESS")) {
                    logger.info("SUCCESS: MetaData updated for KVServer: " + node.getNodeName());
                }
                else if(response.getMsg().equals("UPDATE_FAILED")) {
                    logger.error("ERROR: MetaData not updated for KVServer: " + node.getNodeName());
                }
            } catch (IOException ex) {
                logger.error("UPDATE_ERROR: Update for KVServer failed");
            }
                
        }
    }
 
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        //Select node from available, update hashing, add to hashRing and alert all servers to upate metaData 
        IECSNode node = new ECSNode();     
        try { 
            ArrayList<String> metaDataContent = new ArrayList<String>();
            for(Map.Entry<IECSNode, String> entry : allAvailableServers.entrySet()) {
                if(entry.getValue().equals("AVAILABLE")) {
                	//Add the server to the ringNetwork and update HashMap 
                    entry.setValue("TAKEN");
                    node = entry.getKey();
                    String serverHash = md5.encode(node.getNodeName() +  KVConstants.DELIM +
                                               node.getNodeHost() + KVConstants.DELIM +
                                               node.getNodePort());
                    
                    // Add the node to ZK
                	ZKImpl.joinGroup(KVConstants.ZK_ROOT, node.getNodeName());
                    ZKImpl.list(KVConstants.ZK_ROOT);
                	//Update the hashing for all servers in the ring
                    node = updateHash(serverHash, node);
                    //add node to hash Ring
                    ringNetwork.put(serverHash, node);

                    //Prepare writable content to write to metaDatafile    
                    //TODO double check that data per server is written separate lines 
                    metaDataContent.add(node.getNodeName() + KVConstants.DELIM + node.getNodeHost() + KVConstants.DELIM + node.getNodePort() + KVConstants.DELIM + node.getNodeHashRange()[0] + KVConstants.DELIM + node.getNodeHashRange()[1]);     

                }
            } 
            //Once all nodes are added, write to metaDataFile and alert nodes to update their metaData
            updateMetaData(metaDataContent);

        }
        catch (IOException io) {
            logger.error("Unable to write to metaDataFile"); 
        }
        catch (KeeperException | InterruptedException e) {
			logger.error("Unable to join group. Exception: " + e);
        }
        //Other errors ??
        return node; 
    }

    public Collection<IECSNode> initAddNodesToHashRing(int numNodes) {
        Collection<IECSNode> chosenNodes = new ArrayList<IECSNode>();     
        int counter = 0;
    
        try { 
            //Initialize own hashRing
            ArrayList<String> metaDataContent = new ArrayList<String>();
            for(Map.Entry<IECSNode, String> entry : allAvailableServers.entrySet()) {
                if(counter < numNodes) {
                    if(entry.getValue().equals("AVAILABLE")) {
                        counter++;
                        entry.setValue("TAKEN");
                        IECSNode node = entry.getKey();
                        node.printMeta();
                        String serverHash = md5.encode(node.getNodeHost() + KVConstants.HASH_DELIM +
                                                   Integer.toString(node.getNodePort()));
                        //Setup begin and end hashing for server 
                        node = updateHash(serverHash, node);   
                        //Add node to ringNetwork
                        ringNetwork.put(serverHash, node);
                
                        //Prepare writable content to write to metaDatafile    
                        //TODO double check that data per server is written separate lines 
                        metaDataContent.add(node.getNodeName() + KVConstants.DELIM + node.getNodeHost() + KVConstants.DELIM + node.getNodePort() + KVConstants.DELIM
                                            + node.getNodeHashRange()[0] + KVConstants.DELIM + node.getNodeHashRange()[1]);      

                        //Add chosen node to collection
                        chosenNodes.add(node);
                    }
                }
            } 
            //Once all nodes are added, write to metaDataFile
            updateMetaData(metaDataContent);
        } catch (IOException io) {
            logger.error("Unable to write to metaDataFile"); 
        }
        
        return chosenNodes; 
    }

    public Collection<IECSNode> setupNodesCacheConfig(int count, String cacheStrategy, int cacheSize) {
        Collection<IECSNode> nodes = new ArrayList<IECSNode>();
        TextMessage message = new TextMessage("ECS" + KVConstants.DELIM + "SETUP_NODE" + KVConstants.DELIM + cacheStrategy + KVConstants.DELIM + cacheSize); 
        TextMessage response;
        for(Map.Entry<String, IECSNode> entry: ringNetwork.entrySet()) {
            IECSNode node = entry.getValue();
            try {
                response = sendNodeMessage(message, node);                      
                if(response.getMsg().equals("SETUP_SUCCESS")) {
                    logger.info("SUCCESS: Setup for KVServer: " + node.getNodeName()); 
                    nodes.add(node); 
                }
                else if(response.getMsg().equals("SETUP_FAILED")) {
                    logger.error("ERROR: Failed in Setup for KVServer: " + node.getNodeName());
                }
            } catch(IOException ex) {
                logger.error("ERROR: Failed in Setup for KVServer: " + node.getNodeName()); 
            } 
        }
        return nodes;
    }

    public IECSNode updateHash(String nodeHash, IECSNode node) {
    	
    	assert node != null;
        
        IECSNode prevNode = new ECSNode(), currNode = new ECSNode(), nextNode = new ECSNode();
        String eHash = nodeHash;

        // Only one in network, so start and end are yours
        if(ringNetwork.isEmpty()) {
            node.setNodeBeginHash(eHash);
            node.setNodeEndHash(eHash);
            return node;
        }
        else if(ringNetwork.containsKey(nodeHash)) {
            logger.error("ERROR KVServer already in ringNetwork");
            return null;
        }
        else if(ringNetwork.higherKey(nodeHash) == null) {// the current hash is the highest value
            prevNode = ringNetwork.firstEntry().getValue();
            currNode.setNodeBeginHash(prevNode.getNodeHashRange()[0]);   //prevNode authority only goes as far currently added node                     
            prevNode.setNodeBeginHash(nodeHash);
        }
        else { //currNode is at beginning or in between
            nextNode = ringNetwork.higherEntry(nodeHash).getValue();
            currNode.setNodeBeginHash(nextNode.getNodeHashRange()[0]);
            nextNode.setNodeBeginHash(nodeHash);
        }
        return currNode; 
    }

    public boolean launchKVServer(IECSNode node, String cacheStrategy, int cacheSize) {
        //TODO SSH stuff initializing with server's own name and port
        boolean success = true;
        Process proc; 
        Runtime run = Runtime.getRuntime();
        try {
            proc = run.exec("script.sh"/*Script*/);
            Thread.sleep(LAUNCH_TIMEOUT);

        } catch (InterruptedException ex) {
            logger.error("ERROR: KVServer Thread unable to sleep");
            success = false;
        } catch (IOException io) {
            logger.error("ERROR: KVServer IOException: " + io);
        }

        logger.info("KVServer process initiated successfully");
        return success;
    }


    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    public boolean removeNodes(Collection<IECSNode> nodes) {
        // TODO
        String currNodeHash;    
        IECSNode nextNode = null, currNode = null, node;
        boolean success = true;
        ArrayList<String> metaDataContent = new ArrayList<String>();

        for(IECSNode server: nodes) {
            for(Map.Entry<String, IECSNode> entry : ringNetwork.entrySet()) {
                if(server.getNodeName().equals(entry.getValue().getNodeName())) {
                    //Found the node we want to remove
                    currNodeHash = entry.getKey();
                    currNode = entry.getValue();
                    nextNode = findNextNode(currNodeHash);
                    if(nextNode == null) {
                        logger.error("ERROR: Removing node error");
                        success = false;
                    }
                    nextNode.setNodeBeginHash(currNode.getNodeHashRange()[0]);
                    ringNetwork.remove(currNodeHash);
                    allAvailableServers.put(currNode, "AVAILABLE");
                }
            }
        }
       
        try { 
            for(Map.Entry<String, IECSNode> entry : ringNetwork.entrySet()) {
                node = entry.getValue();
                metaDataContent.add(node.getNodeName() + KVConstants.DELIM + node.getNodeHost() + KVConstants.DELIM + node.getNodePort() + KVConstants.DELIM + node.getNodeHashRange()[0] + KVConstants.DELIM + node.getNodeHashRange()[1]);     
            }
            updateMetaData(metaDataContent);
        } catch (IOException io) {
            logger.error("ERROR: Unable to update metaData with nodes removed");
            success = false;
        }

        return success;
    }

    public IECSNode findNextNode(String currHash) {
        IECSNode nextNode = new ECSNode();
        if(ringNetwork.size() <= 1 || !ringNetwork.containsKey(currHash))
            nextNode = null;
        else {
            if(ringNetwork.higherKey(currHash) == null) {
                nextNode = ringNetwork.firstEntry().getValue();
            }
            else {
                nextNode = ringNetwork.higherEntry(currHash).getValue();
            }
        }
        return nextNode; 
    }
    
    public Map<String, IECSNode> getNodes() {
        return this.ringNetwork;
    }

    public IECSNode getNodeByKey(String Key) {
        IECSNode serverNode;
        if(ringNetwork.isEmpty()) return null;
        String encodedKey = md5.encode(Key);
        if(ringNetwork.higherEntry(encodedKey) == null) {
            serverNode = ringNetwork.firstEntry().getValue();
        }
        serverNode = ringNetwork.higherEntry(encodedKey).getValue();
        return serverNode;
    }

    public TextMessage sendNodeMessage(TextMessage message, IECSNode node) throws IOException {
        TextMessage response = new TextMessage("") ; 
            connect(node);
            sendMessage(message);
            response = receiveMessage();
            disconnect(node);
        return response;
    }

    public void connect(IECSNode server) throws IOException {
        this.ECSSocket = new Socket(server.getNodeHost(), server.getNodePort());
        logger.info("ECS Connection to name: " + server.getNodeName() + " successful");
    }

    public void disconnect(IECSNode server) {
        try { 

            if(ECSSocket != null) {
                ECSSocket.close();
                ECSSocket = null;
                logger.info("Successfully closed ECS connection to server: " + server.getNodeName());

            }
        } catch (IOException io) {
            logger.error("Unable to close ECS to server connection");
        }

    }
        
    /**
     * Method sends a TextMessage using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream 
     */
    public void sendMessage(TextMessage msg)
            throws IOException {
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

	@Override
	public boolean shutdown() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Collection<IECSNode> addNodes(int count, String cacheStrategy,
			int cacheSize) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<IECSNode> setupNodes(int count, String cacheStrategy,
			int cacheSize) {
		// TODO Auto-generated method stub
		return null;
	}


}
