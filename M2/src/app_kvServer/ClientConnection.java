package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.math.BigInteger;
import cache.KVCache;
import common.messages.TextMessage;
import common.Parser.*;

import org.apache.log4j.*;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending.
 * The class also implements the get and put functionality. Thus whenever a message 
 * is received with "put key value", it is added to the persistent disk storage and also cached.
 * When a message is received with "get key", its corresponding value is returned.
 */
public class ClientConnection implements Runnable {

    private static Logger logger = Logger.getRootLogger();
    
    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int MAX_KEY_LENGTH = 20; 
    private static final int MAX_VALUE_LENGTH = 122880; //120KB
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    private HashMap<BigInteger, ServerMetaData> ringNetwork;
    
    private Socket clientSocket;
    private KVServer server;
    private InputStream input;
    private OutputStream output;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection (KVServer server, Socket clientSocket) {
        this.server = server;
        this.clientSocket = clientSocket;
        this.isOpen = true;
    }
    
    /**
     * Initializes and starts the client connection. 
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();
        
            sendMessage(new TextMessage(
                    "Connection to KV server established: " 
                    + clientSocket.getLocalAddress() + " / "
                    + clientSocket.getLocalPort()));
            
            while (isOpen) {
                try {
                    TextMessage msgReceived = receiveMessage();
                    // Unmarshalling of received message
                    String[] msgContent = msgReceived.getMsg().split("\\" + DELIM);
                    String command = msgContent[0];
                    // Key cannot contain DELIM, but value can
                    // So combine all strings from msgContent[2] till end to get value
                    List<String> valueParts = new LinkedList<>();
                    for (int i = 2; i < msgContent.length; ++i) {
                        valueParts.add(msgContent[i]);
                    }
                    String value = String.join(DELIM, valueParts);
                    if (command.equals("PUT")) {
                        if(server.isWriteLocked()){
                            sendMessage(new TextMessage("SERVER_WRITE_LOCK"));
                            logger.info("SERVER_WRITE_LOCK: server locked, write operation failed");
                            continue;
                        }
                        handlePutCmd(msgContent[1], value);
                    }
                    else if (command.equals("GET")) {
                        handleGetCmd(msgContent[1]);
                    }
                    else {
                        logger.error("Received invalid message type from client.");
                        System.out.println("Received invalid message type from client.");
                    }
                }
                /* connection either terminated by the client or lost due to 
                 * network problems*/
                catch (IOException ioe) {
                    logger.error("Error! Connection lost!");
                    isOpen = false;
                }               
            }
        }
        catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);
        }
        finally {
            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    clientSocket.close();
                }
            }
            catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
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
        if (key.contains(DELIM)) {
            logger.error("Server Error: Key should not contain delimiter " + DELIM);
            result = false;
        }
        return result;
    }

    private void handlePutCmd (String key, String value) {
        String result = "PUT_ERROR";
        //Done in KVServer;
        if (errorCheck(key, value)) {
            // If Value is not null or empty, insert in $ and disk
            if (value != null && !value.equals("") && !value.equals("null")) {
                try {
                    result = (server.inStorage(key)) ? "PUT_UPDATE" : "PUT_SUCCESS";
                    server.putKV(key, value);
                    logger.info("Success " + result + " with key " + key + " and value " 
                        + value + " on server");
                }
                catch (Exception ex) {
                    result = "PUT_ERROR";
                    logger.error("PUT Error! Unable to add key " + key + " and value "
                        + value + " to server");
                }
            }
            else {
                // Delete the value from $ and disk
                try {
                    result = (server.inStorage(key)) ? "DELETE_SUCCESS" : "DELETE_ERROR";
					if(result.equals("DELETE_SUCCESS")) 
	                    server.deleteKV(key);
                    logger.info("Success " + result + " with key " + key + " on server");
                }
                catch (Exception ex) {
                    result = "DELETE_ERROR";
                    logger.error("DELETE Error! Unable to delete key " + key + " from server");
                }
            }
        }

        // Send PUT Ack to the user
        try {
            sendMessage(new TextMessage(result));
        }
        catch (Exception ex) {
            logger.error("SEND Error! Server unable to send PUT Ack message back to the client");
        }
    }

    private void handleGetCmd (String key) {
        String result = "GET_ERROR";
        if (errorCheck(key, "")) {
            //Done in KVServer;
            try {
                String value = server.getKV(key);
                result = "GET_SUCCESS" + DELIM + value;
                logger.info("Successfully fetched the value " + value + " for the key " + key + " on server");
            }
            catch (Exception ex) {
                result = "GET_ERROR";
                logger.info("GET_ERROR: Unable to fetch the value for the key " + key + " on server");
            }
        }

        // Send PUT Ack to the user
        try {
            sendMessage(new TextMessage(result));
        }
        catch (Exception ex) {
            logger.error("SEND Error! Server unable to send PUT Ack message back to the client");
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
                + clientSocket.getInetAddress().getHostAddress() + ":" 
                + clientSocket.getPort() + ">: '" 
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
                + clientSocket.getInetAddress().getHostAddress() + ":" 
                + clientSocket.getPort() + ">: '" 
                + msg.getMsg().trim() + "'");
        return msg;
    }
}
