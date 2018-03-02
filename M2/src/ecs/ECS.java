package ecs;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;
import java.net.Socket;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import java.io.OutputStream;
import java.io.InputStream;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;


import java.math.BigInteger;
import java.util.Collection;

import org.apache.log4j.Logger;
import java.lang.Process;
import org.apache.zookeeper.ZooKeeper;

import common.*;
import common.messages.TextMessage;

import ecs.CreateGroup;

public class ECS {
    private Path configFile;
    private Path metaDataFile;
    private Socket ECSSocket;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    public static final String metaFile = "metaDataECS.config";            
    
    private TreeMap<String, IECSNode> ringNetwork;
    private static Logger logger = Logger.getRootLogger();
    private OutputStream output; 
    private InputStream input;
        
    private ZooKeeper zk;
    
	public ECS(String configFile) {
        this.configFile = Paths.get(configFile);
        this.ringNetwork = new TreeMap<String, IECSNode>();
        try {
            if(!Files.exists(Paths.get(metaFile)))
                this.metaDataFile = Files.createFile(Paths.get(metaFile));
            else {
			    this.metaDataFile = Paths.get(metaFile);    
                populateRingNetwork();
            }
            
		} catch (IOException e) {
			logger.error("Unable to open metaDataFile " + e);
		}
    }

    private void populateRingNetwork()
    		throws IOException {
        ArrayList<String> lines = new ArrayList<>(Files.readAllLines(this.metaDataFile, StandardCharsets.UTF_8));
        int numServers = lines.size();		
        
        for (int i = 0; i < numServers ; ++i) {
            String[] line = lines.get(i).split(" ");
            String serverHash = md5.encode(line[ServerMetaData.SERVER_NAME] + KVConstants.DELIM +
                                           line[ServerMetaData.SERVER_IP] + KVConstants.DELIM +
                                           line[ServerMetaData.SERVER_PORT]);
            IECSNode node = new ECSNode(line[ServerMetaData.SERVER_NAME], line[ServerMetaData.SERVER_IP], Integer.parseInt(line[ServerMetaData.SERVER_PORT]), line[ServerMetaData.BEGIN_HASH], line[ServerMetaData.END_HASH]);
            this.ringNetwork.put(serverHash, node);
        }
    }

    public int maxServers() {
    	return ringNetwork.size();
    }

    public boolean start(IECSNode server) {
        // TODO
        boolean failed = false;
        TextMessage message, response; 
            
//        for(Map.Entry<String, IECSNode> iter: ringNetwork.entrySet()) {
            try { 
//                server = iter.getValue();
                message = new TextMessage("ECS" + KVConstants.DELIM + "START_NODE");
                connect(server);
                sendMessage(message);
                response  = receiveMessage();
                disconnect(server);
                if(response.getMsg().equals("START_SUCCESS")) 
                    logger.info("Started KVServer: " + server.getNodeName() + " <" + server.getNodeHost() + "> <" + 
                    server.getNodePort() + ">"); 
            } catch (IOException io) {
			    logger.error("START ERROR for KVServer: " + server.getNodeName() + ". Error: " + io);
                failed = true;
            } 
           
  //      }
            
        return failed;
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
        
        
    public boolean stop(IECSNode server) {
        // TODO
        boolean failed = false;
        TextMessage message, response; 
        
        try { 
            message = new TextMessage("ECS" + KVConstants.DELIM + "STOP_NODE");
            connect(server);
            sendMessage(message);
            response = receiveMessage();
            disconnect(server);
            if(response.getMsg().equals("STOP_SUCCESS")) 
                logger.info("Stop KVServer: " + server.getNodeName() + " <" + server.getNodeHost() + "> <" + 
                server.getNodePort() + ">"); 
        } catch (IOException ex) {
		    logger.error("STOP ERROR for KVServer: " + server.getNodeName() + ". Error: " +  ex);
            failed = true;
        } 
           
            
        return failed;
    }


    public boolean shutdown() {
        // TODO
            
        return false;
    }

    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        //Add a node in own metadata, reassign hashing and send a TextMessage to all Servers 
        // to update their metaData (UPDATE_METADATA)
        return null;
    }

    public boolean initService(Collection<IECSNode> nodes, String cacheStrategy, int cacheSize) {
        // TODO
    
       // Process proc;
       // String script = "script.sh";
        boolean success = true;

       // for(IECSNode node: nodes) {
       //     //SSHCall for each server and launch with the right size and strategy
       //     Runtime run = Runtime.getRuntime();
       //     String nodeHost = node.getNodeHost();
       //     String nodePort = Integer.toString(node.getNodePort());
       //     String cmd = "script " + nodeHost + " " + nodePort;
       //     try {
       //       proc = run.exec(cmd);
       //     } catch (IOException e) {
       //       e.printStackTrace();
       //     }
       // }

        //Initialize own hashRing

        return success; 
    }

    public Collection<IECSNode> connectToZk(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    public Map<String, IECSNode> getNodes() {
        // TODO
        return this.ringNetwork;
    }

    public IECSNode getNodeByKey(String Key) {
        // TODO
        IECSNode serverNode;
        if(ringNetwork.isEmpty()) return null;
        String encodedKey = md5.encode(Key);
        if(ringNetwork.higherEntry(encodedKey) == null) {
            serverNode = ringNetwork.firstEntry().getValue();
        }
        serverNode = ringNetwork.higherEntry(encodedKey).getValue();
        return serverNode;
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


}
