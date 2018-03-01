package client;

import java.io.InputStream;
import java.io.OutputStream;

import java.net.Socket;

import java.util.HashSet;
import java.util.Set;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;

import java.math.BigInteger;

import java.net.UnknownHostException;
import java.io.IOException;

import org.apache.log4j.Level;
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
    private HashMap<BigInteger, ServerMetaData> ringNetwork;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private static final int MAX_KEY_LENGTH = 20; 
    private static final int MAX_VALUE_LENGTH = 122880; //120KB
    private static final BigInteger INVALID_SERVER = BigInteger.valueOf(-1);

    /**
     * Initialize KVStore with address and port of KVServer
     * @param address the address of the KVServer
     * @param port the port of the KVServer
     */
    public KVStore(String address, int port) {
        this.serverAddr = address;
        this.serverPort = port;
        setRunning(true);
    }

    @Override
    public void connect() 
            throws UnknownHostException, IOException {
        this.clientSocket = new Socket(this.serverAddr, this.serverPort);
        this.listeners = new HashSet<IKVClient>();
        this.output = clientSocket.getOutputStream();
        this.input = clientSocket.getInputStream();

        // Receive the connection ack message
        TextMessage reply = receiveMessage();
        logger.info(reply.getMsg());
    }

    @Override
    public synchronized void disconnect() {
        // TODO Auto-generated method stub
        logger.info("Trying to close connection ...");
        
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
        // step 1 - input validation
        if (!errorCheck(key, value)) {
            return new KVReplyMessage(key, value, KVMessage.StatusType.PUT_ERROR);
        }

        // step 2 - send a PUT request to the server
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
            case SERVER_NOT_RESPONSIBLE:
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

        // step 2 - send a PUT request to the server
        TextMessage message = new TextMessage(KVConstants.GET_CMD + KVConstants.DELIM + key);
        sendMessage(message);

        // step 3 - get the server's response and forward it to the client
        TextMessage reply = receiveMessage();
        String[] tokens = (reply.getMsg()).split("\\|");
        String getStatus = tokens[0];
        
        if (tokens.length < 2) {
            return new KVReplyMessage(key, null, KVMessage.StatusType.GET_ERROR);
        }
        else if (getStatus.equals("GET_SUCCESS")) {
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

    private KVReplyMessage retryRequest(String key, String value, String request)
    		throws Exception {
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
            
            // Find responsible server
            BigInteger serverHash = getResponsibleServer(key);
            if (serverHash == INVALID_SERVER) {
                return new KVReplyMessage(key, value, status);
            }
            
            // Disconnect from current server and reconnect
            // to the correct server
            disconnect();
            this.serverAddr = this.ringNetwork.get(serverHash).addr;
            this.serverPort = this.ringNetwork.get(serverHash).port;
            connect();
            
            return (request.equals(KVConstants.PUT_CMD)) ? put(key, value) : get(key);
        }
    }

    private void updateMetaData(String marshalledData) {
        this.ringNetwork = new HashMap<BigInteger, ServerMetaData>();
        String[] dataEntries = marshalledData.split(KVConstants.NEWLINE_DELIM);

        for(int i = 0; i < dataEntries.length ; i++) {
            BigInteger serverHash = md5.encode(line[0] + KVConstants.DELIM + line[1] + KVConstants.DELIM + line[2]);
            ServerMetaData meta = new ServerMetaData(line[0], line[1], Integer.parseInt(line[2]), line[BEGIN_HASH], line[END_HASH]);
            this.ringNetwork.put(serverHash, meta);
        }
    }

    private BigInteger getResponsibleServer(String key) {
        if(ringNetwork.isEmpty()) return INVALID_SERVER;
        BigInteger encodedKey = md5.encode(key);
        /*
            TODO this comment is copied verbatum
            Return the server that has the next highest hash to the encodedKey.
            If encodedKey has a hash higher than all KVServers, then return
            the Metadata of KVServer with the lowest hash (due to wrap-around).
        */
        if(ringNetwork.higherEntry(encodedKey) == null) {
            return ringNetwork.firstEntry().getValue();
        }
        return ringNetwork.higherEntry(encodedKey).getValue();
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

    private TextMessage receiveMessage()
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
