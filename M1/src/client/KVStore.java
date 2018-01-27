package client;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.LinkedList;
import java.util.List;

import common.messages.KVMessage;
import common.messages.TextMessage;
import common.messages.KVReplyMessage;
import app_kvClient.IKVClient;
import app_kvClient.IKVClient.SocketStatus;

import java.net.UnknownHostException;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVStore implements KVCommInterface {
    private static Logger logger = Logger.getRootLogger();
    private Set<IKVClient> listeners;
    
    private String serverAddr;
    private int serverPort;
    private boolean running;

    private Socket clientSocket;
    private OutputStream output;
    private InputStream input;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private static final int MAX_KEY_LENGTH = 20; 
    private static final int MAX_VALUE_LENGTH = 122880; //120KB
    private static final String DELIM = "|";
    private static final String PUT_CMD = "PUT";
    private static final String GET_CMD = "GET";

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

    @Override
    public KVMessage put(String key, String value)
            throws Exception {
        // step 1 - input validation
        if (key.length() > MAX_KEY_LENGTH) {
            logger.error("Error: maximum key length allowed is " + MAX_KEY_LENGTH + " but key has length " + key.length());
            return new KVReplyMessage(key, value, KVMessage.StatusType.PUT_ERROR);
        }
        if (value.length() > MAX_VALUE_LENGTH) {
            logger.error("Error: maximum value length allowed is 120K Bytes but value has length " + value.length());
            return new KVReplyMessage(key, value, KVMessage.StatusType.PUT_ERROR);
        }
        if (key.contains(" ")) {
            logger.error("Error: Key should not contain space");
            return new KVReplyMessage(key, value, KVMessage.StatusType.PUT_ERROR);
        }
        if (key.contains(DELIM)) {
            logger.error("Error: Key should not contain delimiter " + DELIM);
            return new KVReplyMessage(key, value, KVMessage.StatusType.PUT_ERROR);
        }

        // step 2 - send a PUT request to the server
        // Marshall the sending message
        String msg = PUT_CMD + DELIM + key;
        if (value != null && !value.equals("")) {
            msg = msg + DELIM + value;
        }
        TextMessage message = new TextMessage(msg);
        sendMessage(message);

        // step 3 - get the server's response and forward it to the client
        TextMessage reply = receiveMessage();
        return new KVReplyMessage(key, value, reply.getMsg());
    }

    @Override
    public KVMessage get(String key)
            throws Exception {
        // step 1 - input validation
        if (key.length() > MAX_KEY_LENGTH) {
            logger.error("Error: maximum key length allowed is " + MAX_KEY_LENGTH + " but key has length " + key.length());
            return new KVReplyMessage(key, null, KVMessage.StatusType.GET_ERROR);
        }
        if (key.contains(" ")) {
            logger.error("Error: Key should not contain space");
            return new KVReplyMessage(key, null, KVMessage.StatusType.GET_ERROR);
        }
        // Might not need this - Need to test!!
        if (key.contains(DELIM)) {
            logger.error("Error: Key/Value should not contain delimiter " + DELIM);
            return new KVReplyMessage(key, null, KVMessage.StatusType.GET_ERROR);
        }

        // step 2 - send a PUT request to the server
        TextMessage message = new TextMessage(GET_CMD + DELIM + key);
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
            String value = String.join(DELIM, valueParts);
            return new KVReplyMessage(key, value, KVMessage.StatusType.GET_SUCCESS);
        }
        else {
            // Invalid Message type received or received GET_ERROR
            return new KVReplyMessage(key, null, KVMessage.StatusType.GET_ERROR);
        }
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
