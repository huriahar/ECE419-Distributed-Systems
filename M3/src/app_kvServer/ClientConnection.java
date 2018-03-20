package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Arrays;

import app_kvServer.IKVServer.ReplicaDataAction;
import common.messages.TextMessage;
import common.KVConstants;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.nio.file.*;

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
                    String[] msgContent = msgReceived.getMsg().split("\\" + KVConstants.DELIM);
                    String command = msgContent[0];
                    // Key cannot contain DELIM, but value can
                    // So combine all strings from msgContent[2] till end to get value
                    List<String> valueParts = new LinkedList<>();
                    for (int i = 2; i < msgContent.length; ++i) {
                        valueParts.add(msgContent[i]);
                    }
                    String value = String.join(KVConstants.DELIM, valueParts);
                    String key = null;
                    boolean success = true;
                    if (command.equals("ECS")) {
                        String[] ecsCmd = Arrays.copyOfRange(msgContent, 1, msgContent.length);
                        handleECSCmd(ecsCmd);
                        continue;
                    }
                    else if (command.equals("MOVE_KVPAIRS")) {
                        // Receiving KVPairs from another server
                        System.out.println("************Received Move KV Pairs from someone**********");
                        String destination = msgContent[1];
                        System.out.println("New destination is:" + destination);
                        System.out.println("Initial path is:" + this.server.getCurrFilePath());
                        if(destination.equals("COORDINATOR")) {
                            this.server.setCurrFilePath(this.server.getServerFilePath());
                        }
                        else if (destination.equals(KVConstants.PREPLICA)) {
                            // TODO DELETE THE current pReplica file
                            String pReplicaFile = this.server.getPReplicaFilePath();
                            System.out.println("pReplica File is:" + pReplicaFile);
                            try {
                                Files.deleteIfExists(Paths.get(pReplicaFile));
                            } catch (NoSuchFileException x) {
                                System.out.println("No such file or directory");
                            } catch (DirectoryNotEmptyException x) {
                                System.out.println("Directory not empty");
                            } catch (IOException x) {
                                // File permission problems are caught here.
                                System.out.println("Permissions problems");
                            }
                            System.out.println("Delete old file");
                            this.server.setCurrFilePath(pReplicaFile);
                        }
                        else if (destination.equals(KVConstants.SREPLICA)) {
                            // TODO DELETE THE current sReplica file
                            String sReplicaFile = this.server.getSReplicaFilePath();
                            try {
                                Files.deleteIfExists(Paths.get(sReplicaFile));
                            } catch (NoSuchFileException x) {
                                System.out.println("No such file or directory");
                            } catch (DirectoryNotEmptyException x) {
                                System.out.println("Directory not empty");
                            } catch (IOException x) {
                                // File permission problems are caught here.
                                System.out.println("Permissions problems");
                            }
                            
                            this.server.setCurrFilePath(this.server.getSReplicaFilePath());
                        }
                        System.out.println("File path before call is:" + this.server.getCurrFilePath());

                        if(msgContent.length > 2){
                            success = handleMoveKVPairs(value);
                            String res = success ? "MOVE_SUCCESS" : "MOVE_FAILED";
                            sendMessage(new TextMessage(res));
                            logger.info(res);
                            this.server.setCurrFilePath(this.server.getServerFilePath());
                            System.out.println("End File path is:" + this.server.getCurrFilePath());
                            continue;
                        }                        
                    }
                    else if (command.equals("UPDATE")) {
                        System.out.println("Received UPDATE msg");
                        String destination = msgContent[1];
                        String action = msgContent[2];
                        String updateKey = msgContent[3];
                        List<String> updateValueParts = new LinkedList<>();
                        for (int i = 4; i < msgContent.length; ++i) {
                            updateValueParts.add(msgContent[i]);
                        }
                        String updateValue = String.join(KVConstants.DELIM, updateValueParts);
                        success = server.handleUpdateKVPair(destination, action, updateKey, updateValue);
                        String res = success ? "UPDATE_SUCCESS" : "UPDATE_FAILED";
                        logger.info(res);
                        sendMessage(new TextMessage(res));
                        continue;
                    }
                    else {
                        if(msgContent.length > 1){
                            key = msgContent[1];
                        }
                        logger.debug("step 6");
                        //Just a guard
                        if(key == null) {
                            logger.debug("key is null");
                            continue;
                        }
                        System.out.println("Is server stopped? "  + server.isStopped());
                        if (server.isStopped()) {
                            sendMessage(new TextMessage("SERVER_STOPPED"));
                            logger.info("SERVER_STOPPED cannot handle client requests at the moment.");
                            continue;
                        }
                        System.out.println("Is server responsible? "  + server.isResponsible(key));
                        // Check if server is responsible for this key
                        if (!server.isResponsible(key)){
                            sendMessage(new TextMessage("SERVER_NOT_RESPONSIBLE"));
                            TextMessage getMetaData = receiveMessage();
                            if(getMetaData.getMsg().equals("GET_METADATA")) {
                                sendMessage(new TextMessage(server.getMetaDataFromFile()));
                            } else {
                                logger.info("ERROR!!! EXPECTED TO RECEIVE GET_METADATA MSG!!");
                            }
                            continue;
                        }
                        if (command.equals("PUT")) {
                            if(server.isWriteLocked()){
                                sendMessage(new TextMessage("SERVER_WRITE_LOCK"));
                                logger.info("SERVER_WRITE_LOCK: server locked, write operation failed");
                                continue;
                            }
                            handlePutCmd(key, value);
                        }
                        else if (command.equals("GET")) {
                            if(!server.isReadLocked()) {
                                handleGetCmd(key);
                            } else {
                                sendMessage(new TextMessage("SERVER_STOPPED"));
                                logger.info("SERVER_READ_LOCKED cannot handle client requests at the moment.");
                                continue;
                            }
                        }
                        else {
                            logger.error("Received invalid message type from client.");
                            System.out.println("Received invalid message type from client.");
                        }
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
        if (key.contains(KVConstants.DELIM)) {
            logger.error("Server Error: Key should not contain delimiter " + KVConstants.DELIM);
            result = false;
        }
        return result;
    }

    private boolean handleMoveKVPairs(String kvpairs) {
        if(kvpairs == null) return true;
        logger.debug(kvpairs);
        String[] KVPairs = kvpairs.split(KVConstants.NEWLINE_DELIM);
        for(int i = 0; i < KVPairs.length ; i++) {
            String[] kvpair = KVPairs[i].split("\\" + KVConstants.DELIM);
            logger.debug("MOVING KVPAIR " + kvpair[0] + kvpair[1]);
            try {
                server.putKV(kvpair[0], kvpair[1]);
            } catch (Exception e) {
                logger.error("failed to move KVpair (" + kvpair[0] + ", " + kvpair[1] + ") to server " + server.getHostname());
                return false;
            }
        }
        return true;
    }

    private void handleECSCmd (String[] msg) {
        boolean success;
        try {
            switch(msg[0]) {
                case "WRITE_UNLOCK":
                    server.unlockWrite();
                    sendMessage(new TextMessage("UNLOCK_SUCCESS"));
                    return;
                case "WRITE_LOCK":
                    server.lockWrite();
                    sendMessage(new TextMessage("LOCK_SUCCESS"));
                    return;
                case "SETUP_NODE":
                    int cacheSize = Integer.parseInt(msg[2]);
                    server.setupCache(cacheSize, msg[1]);
                    sendMessage(new TextMessage("SETUP_SUCCESS"));
                    return;
                case "START_NODE":
                    server.start();
                    sendMessage(new TextMessage("START_SUCCESS"));
                    return;
                case "STOP_NODE":
                    server.stop();
                    sendMessage(new TextMessage("STOP_SUCCESS"));
                    return;
                case "SHUTDOWN_NODE":
                    server.shutdown();
                    sendMessage(new TextMessage("SHUTDOWN_SUCCESS"));
                    return;
                case "MOVE_ALL_KVPAIRS":
                    server.setMoveAll(true);
                case "MOVE_KVPAIRS":
                    //targetRange in msg[1], msg[2]
                    System.out.println("Updated metadata. Now moving data");
                    String[] targetRange = Arrays.copyOfRange(msg, 2, 2);
                    String targetName = msg[1];
                    success = server.moveData(targetRange, targetName);
                    if(success) {
                        sendMessage(new TextMessage("MOVE_SUCCESS"));
                    } else {
                        sendMessage(new TextMessage("MOVE_FAILED"));
                    }
                    server.setMoveAll(false);
                    return;
                case "UPDATE_METADATA":
                    success = server.updateMetaData();
                    if(success) {
                        sendMessage(new TextMessage("METADATA_UPDATE_SUCCESS"));
                    } else {
                        sendMessage(new TextMessage("METADATA_UPDATE_FAILED"));
                    }
                    return;
                case "UPDATE_REPLICAS":
                    server.setMoveAll(true);
                    System.out.println("Here at UPDATE_REPLICAS");
                    String primaryReplica = msg[1];
                    String secondaryReplica = msg[2];
                    System.out.println("Replicas received: " + primaryReplica + " " + secondaryReplica);
                    success = server.updateReplicas(primaryReplica, secondaryReplica);
                    if(success) {
                        System.out.println("REPLICAS UPDATED");
                        sendMessage(new TextMessage("REPLICA_UPDATE_SUCCESS"));
                    } else {
                        System.out.println("REPLICAS NOT UPDATED");
                        sendMessage(new TextMessage("REPLICA_UPDATE_FAILED"));
                    }
                    server.setMoveAll(false);
                    return;
                default:
                    logger.error("Unknown ECS cmd!");
                    return;
            } 
        }
        catch (Exception ex) {
            logger.error("ERROR! could not send ACK back to ECS!");
        }

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
                        + value + " to server " + ex);
                    ex.printStackTrace();
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
                result = (value.equals(""))? "GET_ERROR": ("GET_SUCCESS" + KVConstants.DELIM + value);
                if(result.equals("GET_ERROR")) {
                    logger.info("GET_ERROR: Unable to fetch the value for the key " + key + " on server");
                }
                else {
                    logger.info("Successfully fetched the value " + value + " for the key " + key + " on server");
                }
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
