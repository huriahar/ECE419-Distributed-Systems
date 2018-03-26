package client;

import java.io.InputStream;
import java.io.OutputStream;

import java.math.BigInteger;
import java.net.Socket;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.Random;
import java.util.ArrayList;

import java.net.UnknownHostException;
import java.io.IOException;

import org.apache.log4j.Logger;

import common.*;
import common.messages.*; 
import app_kvClient.*;

public class KVStore implements KVCommInterface {
    private static Logger logger = Logger.getRootLogger();
    private Set<IKVClient> listeners;
    
    private String serverAddr;
    private int serverPort;
    private boolean running;
    private boolean connected;

    private Socket clientSocket;
    private OutputStream output;
    private InputStream input;
    private TreeMap<BigInteger, ServerMetaData> ringNetwork;
    private TreeMap<Integer, ServerMetaData> pReplicas;
    private TreeMap<Integer, ServerMetaData> sReplicas;
    private Random rand;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private static final int MAX_KEY_LENGTH = 20; 
    private static final int MAX_VALUE_LENGTH = 122880; //120KB

    /**
     * Initialize KVStore with address and port of KVServer
     * @param address the address of the KVServer
     * @param port the port of the KVServer
     */
    public KVStore(String address, int port) {
        this.serverAddr = address;
        this.serverPort = port;
        this.ringNetwork = new TreeMap<BigInteger, ServerMetaData>();
        this.pReplicas = new TreeMap<Integer, ServerMetaData>();
        this.sReplicas = new TreeMap<Integer, ServerMetaData>();
        this.rand = new Random();
        // Add the server address and port to the hash ring
        BigInteger serverHash = md5.encode(address + KVConstants.HASH_DELIM +
                Integer.toString(port));

        ServerMetaData serverNode = new ServerMetaData(null, address, port, null, null);
		// Setup begin and end hashing for server
		serverNode = updateMetaData(serverHash, serverNode);
		ringNetwork.put(serverHash, serverNode);
    }

	public ServerMetaData updateMetaData(BigInteger nodeHash, ServerMetaData currNode) {
		
		assert currNode != null;

        ServerMetaData nextNode = new ServerMetaData();
        // Only one in network, so start and end are yours
        if(ringNetwork.isEmpty()) {
            currNode.setBeginHash(nodeHash);
            currNode.setEndHash(nodeHash);
            return currNode;
        }
        else if(ringNetwork.containsKey(nodeHash)) {
            logger.error("ERROR KVServer already in ringNetwork");
            return null;
        }
        
        // the current hash is the highest value
        if(ringNetwork.higherKey(nodeHash) == null) {
            nextNode = ringNetwork.firstEntry().getValue();
        }
        else {
        	//currNode is at beginning or in between
            nextNode = ringNetwork.higherEntry(nodeHash).getValue();
        }
        // nextNode authority only goes as far currently added node
        currNode.setBeginHash(nextNode.getHashRange()[0]);
        currNode.setEndHash(nodeHash);
        nextNode.setBeginHash(nodeHash);
        //Update the ringNetwork with the changed hash of the next one as well
        ringNetwork.put(nextNode.getHashRange()[1], nextNode);
        return currNode; 
    }

    private boolean isConnected(String addr, int port) {
        logger.debug("in isConnected.. checking for addr " + addr + " port " + port);
        return (this.serverAddr.equals(addr) && this.serverPort == port);
    }
	
	public void connectToResponsibleServer(String key, String cmd) {
		BigInteger responsibleServerKey = getResponsibleServer(key);
		ServerMetaData responsibleServerMeta = ringNetwork.get(responsibleServerKey);
        if(responsibleServerMeta == null) {
            //TODO make sure that after the coordinator crashes, client doesn't get stuck here!
            logger.error("Responsible coordinator not on the ring network!");
            return;
        }
        List<ServerMetaData> options = new ArrayList<ServerMetaData>();
        options.add(responsibleServerMeta); 
        //choose randomly between connecting to the coordinator or its replicas to distribute
        //the load
        ServerMetaData pReplica = pReplicas.get(responsibleServerMeta.getServerPort());
        ServerMetaData sReplica = sReplicas.get(responsibleServerMeta.getServerPort());
        if(pReplica != null) options.add(pReplica);
        if(sReplica != null) options.add(sReplica);
		if ((isConnected(responsibleServerMeta.getServerAddr(), responsibleServerMeta.getServerPort())) ||
            (pReplica != null && cmd.equals(KVConstants.GET_CMD) && isConnected(pReplica.getServerAddr(), pReplica.getServerPort())) ||
            (sReplica != null && cmd.equals(KVConstants.GET_CMD) && isConnected(sReplica.getServerAddr(), sReplica.getServerPort()))) {
			// Do nothing. Already connected to the responsible server, either the coordinator
            // or one of its replicas
			return;
		}
		else {
            //Randomly choose which one to connect to
            int randomNum = rand.nextInt(options.size()); 
            if(cmd.equals(KVConstants.PUT_CMD)) randomNum = 0;   //if this is a PUT, always connect to the coordinator, not one of the replicas
            logger.debug("Choosing between COORD and REPLICAS.... randomNum is " + randomNum  + " and options size is " + options.size());
            assert randomNum >= 0 && randomNum < options.size();
			disconnect();
			this.serverAddr = options.get(randomNum).getServerAddr();
			this.serverPort = options.get(randomNum).getServerPort();
			try {
				connect();
			} catch (IOException e) {
				logger.error("Unable to connect to " + serverAddr + " at " + serverPort);
			}
		}
	}

    @Override
    public void connect() 
            throws UnknownHostException, IOException {
        this.clientSocket = new Socket(this.serverAddr, this.serverPort);
        this.listeners = new HashSet<IKVClient>();
        this.output = clientSocket.getOutputStream();
        this.input = clientSocket.getInputStream();
        setRunning(true);

        // Receive the connection ack message
        TextMessage reply = receiveMessage();
        logger.info(reply.getMsg());
    }

    @Override
    public synchronized void disconnect() {
        try {
            tearDownConnection();
            for (IKVClient listener : listeners) {
                listener.handleStatus(IKVClient.SocketStatus.DISCONNECTED);
            }
        } 
        catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }

    private void tearDownConnection() 
            throws IOException {
        setRunning(false);
        logger.info("Tearing down the connection ...");
        if (clientSocket != null) {
            input.close();
            output.close();
            clientSocket.close();
            clientSocket = null;
            logger.info("Connection closed!");
        }
    }

    public int getServerPort() {
        return this.serverPort;
    }

    private boolean errorCheck (String key, String value) {
        boolean result = true;
        if (key.length() < 1) {
            logger.error("Server Error: minimum key length allowed is 1 but key has length " + key.length());
            result = false;
        }
        if (key.length() > MAX_KEY_LENGTH) {
            logger.error("Server Error: maximum key length allowed is " + MAX_KEY_LENGTH + " but key has length " + key.length());
            result = false;
        }
        if (value.length() > MAX_VALUE_LENGTH) {
            logger.error("Server Error: maximum value length allowed is 120K Bytes but value has length " + value.length());
            result = false;
        }
        if (key.contains(" ")) {
            logger.error("Server Error: Key should not contain space");
            result = false;
        }
        if (key.contains(KVConstants.DELIM)) {
            logger.error("Server Error: Key should not contain delimiter " + KVConstants.DELIM);
            result = false;
        }
        return result;
    }

    @Override
    public KVReplyMessage put(String key, String value)
            throws Exception {
        logger.debug("in put: key  = " + key + " value  " + value);
        // step 1 - input validation
        if (!errorCheck(key, value)) {
            logger.debug("error check failed in put: key  = " + key + " value  " + value);
            return new KVReplyMessage(key, value, KVMessage.StatusType.PUT_ERROR);
        }
        
        // Step2 - Figure out which server is responsible based on the information 
        // that KVStore has and connect to it
        connectToResponsibleServer(key, KVConstants.PUT_CMD);

        // step 3 - send a PUT request to the server
        // Marshall the sending message
        String msg = KVConstants.PUT_CMD + KVConstants.DELIM + key;
        if (value != null && !value.equals("")) {
            msg = msg + KVConstants.DELIM + value;
        }
        TextMessage message = new TextMessage(msg);
        sendMessage(message);

        // step 3 - get the server's response and forward it to the client
        TextMessage reply = receiveMessage();
        KVReplyMessage kvreply = new KVReplyMessage(key, value, reply.getMsg());
        
        // step 4 - retry put if possible
        switch(kvreply.getStatus()){
            //TODO if server is stopped, do you return PUT/GET failed or SERVER_STOPPED?
        	// This means that my metaData on servers is incorrect - so handle that
            case SERVER_NOT_RESPONSIBLE:
                logger.debug("SERVER is not responsible.... finding which one is...");
                kvreply = retryRequest(key, value, KVConstants.PUT_CMD);
            default:
                break;
        }
        return kvreply;
    }

    @Override
    public KVReplyMessage get(String key)
            throws Exception {
        // step 1 - input validation
        if (!errorCheck(key, "")) {
            return new KVReplyMessage(key, null, KVMessage.StatusType.GET_ERROR);
        }

        connectToResponsibleServer(key, KVConstants.GET_CMD);
        // step 2 - send a PUT request to the server
        TextMessage message = new TextMessage(KVConstants.GET_CMD + KVConstants.DELIM + key);
        sendMessage(message);

        // step 3 - get the server's response and forward it to the client
        TextMessage reply = receiveMessage();
        String[] tokens = (reply.getMsg()).split("\\" + KVConstants.DELIM);
        String getStatus = tokens[0];
        
//        if (tokens.length < 2) {
//            return new KVReplyMessage(key, null, KVMessage.StatusType.GET_ERROR);
//        }
        if (getStatus.equals("GET_SUCCESS")) {
            // Success! Combine the remaining tokens to get value
            // Done as value can contain DELIM
            List<String> valueParts = new LinkedList<>();
            for (int i = 1; i < tokens.length; ++i) {
                valueParts.add(tokens[i]);
            }
            String value = String.join(KVConstants.DELIM, valueParts);
            return new KVReplyMessage(key, value, KVMessage.StatusType.GET_SUCCESS);
        }
        else if(getStatus.equals("SERVER_NOT_RESPONSIBLE")) {
            return retryRequest(key, null, KVConstants.GET_CMD);
        }
        else {
            // Invalid Message type received or received GET_ERROR
            return new KVReplyMessage(key, null, KVMessage.StatusType.GET_ERROR);
        }
    }

    private void debugPrint(String m) {
        System.out.println("DEBUG KVSTORE: " + m);
    }

    private KVReplyMessage retryRequest(String key, String value, String request)
            throws Exception {
        logger.debug("retrying " + request + " for kvp (" + key + ", " + value + ")");
        // step 1 - Update ServerMetaData
        String status = (value == null) ? "DELETE_ERROR" : "PUT_ERROR";
        status = (request.equals(KVConstants.GET_CMD)) ? "GET_ERROR" : status;

        sendMessage(new TextMessage("GET_METADATA"));
        TextMessage reply = receiveMessage();
        if(reply.getMsg().equals("METADATA_FETCH_ERROR")) {
            return new KVReplyMessage(key, value, status);
        }
        else {
            // Update ServerMetaData
            updateMetaData(reply.getMsg());
            //receive the second msg with the replica information
            reply = receiveMessage();
            if(!reply.getMsg().equals("REPLICA_FETCH_ERROR")) {
                updateReplicaInformation(reply.getMsg());
            }
            return (request.equals(KVConstants.PUT_CMD)) ? put(key, value) : get(key);
            
        }
    }

    private ServerMetaData findServerInRingNetwork(Integer port) {
        for(Map.Entry<BigInteger, ServerMetaData> entry : ringNetwork.entrySet()) {
            ServerMetaData node = entry.getValue();
            if(node.getServerPort() == port) {
               return node;
            }
        }
        return null;
    }

    private void updateReplicaInformation(String marshalledData) {
        String[] dataEntries = marshalledData.split(KVConstants.NEWLINE_DELIM);
        for(int i = 0; i < dataEntries.length ; ++i) {
            String[] entry = dataEntries[i].split(KVConstants.SPLIT_DELIM);
            this.pReplicas.put(Integer.parseInt(entry[0]), findServerInRingNetwork(Integer.parseInt(entry[1])));
            this.sReplicas.put(Integer.parseInt(entry[0]), findServerInRingNetwork(Integer.parseInt(entry[2])));
        } 
    }

    private void updateMetaData(String marshalledData) {
        this.ringNetwork = new TreeMap<BigInteger, ServerMetaData>();
        String[] dataEntries = marshalledData.split(KVConstants.NEWLINE_DELIM);
        for(int i = 0; i < dataEntries.length ; ++i) {
            ServerMetaData meta = new ServerMetaData(dataEntries[i]);
            BigInteger serverHash = md5.encode(meta.getServerAddr() + KVConstants.HASH_DELIM + meta.getServerPort());

            this.ringNetwork.put(serverHash, meta);
        }
        printRing();
    }
    
    public void printRing() {
        ServerMetaData node;
        System.out.println("Printing ring network-------------------------------------------------");
        for(Map.Entry<BigInteger, ServerMetaData> entry : ringNetwork.entrySet()) {
            node = entry.getValue();
            System.out.println(node.getServerAddr() + " : " + node.getServerPort() + " : " + node.getHashRange()[0] + " : " + node.getHashRange()[1]);
        }
        System.out.println("Done printing ring network--------------------------------------------");
    }

    private BigInteger getResponsibleServer(String key) {
        if(ringNetwork.isEmpty()) return null;
        BigInteger encodedKey = md5.encode(key);
        /*
            TODO this comment is copied verbatum
            Return the server that has the next highest hash to the encodedKey.
            If encodedKey has a hash higher than all KVServers, then return
            the Metadata of KVServer with the lowest hash (due to wrap-around).
        */
        if(ringNetwork.higherEntry(encodedKey) == null) {
            return ringNetwork.firstEntry().getKey();
        }
        return ringNetwork.higherEntry(encodedKey).getKey();
    }


    public boolean isConnected() {
        return connected;
    }
    
    public void setConnected(boolean connect) {
        connected = connect;
    }
    
    public boolean isRunning() {
        return running;
    }
    
    public void setRunning(boolean run) {
        running = run;
    }

    public void addListener(IKVClient listener){
        listeners.add(listener);
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
        logger.info("Send message:\t '" + msg.getMsg() + "'");
    }

    public TextMessage receiveMessage()
            throws IOException {
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];
        
        /* read first char from stream */
        byte read = (byte) input.read();    
        boolean reading = true;
        
        while (read != 13 && reading) {/* carriage return */
            /* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                if (msgBytes == null) {
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
            
            /* only read valid characters, i.e. letters and numbers */
            if ((read > 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }
            
            /* stop reading is DROP_SIZE is reached */
            if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }
            
            /* read next char from stream */
            read = (byte) input.read();
        }
        
        if (msgBytes == null) {
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        }
        else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }
        
        msgBytes = tmp;
        
        /* build final String */
        TextMessage msg = new TextMessage(msgBytes);
        logger.info("Receive message:\t '" + msg.getMsg() + "'");
        return msg;
    }
}
