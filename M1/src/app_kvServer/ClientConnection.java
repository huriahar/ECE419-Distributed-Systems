package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.io.FileWriter;  
import java.nio.file.Path;

//import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import cache.KVCache;
import common.messages.TextMessage;

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
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    private static final String DELIM = "|";
    
    private Socket clientSocket;
    private KVServer server;
    private InputStream input;
    private OutputStream output;
    private Path storagePath;
    private KVCache cache;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection (KVServer server, Socket clientSocket, Path storagePath, KVCache cache) {
        this.server = server;
        this.clientSocket = clientSocket;
        this.isOpen = true;
        this.storagePath = storagePath;
//      this.cache = cache;
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
                    if (command.equals("PUT")) {
                        handlePutCmd(msgContent[1], msgContent[2]);
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

    private void handlePutCmd (String key, String value) {
        String result = "";
        //Done in KVServer;
        System.out.println("PUT Key " + key + " Value " + value);
        // If Value is not null or empty, insert in $ and disk
        if (value != null && !value.equals("") && !value.equals("null")) {
            try {
                server.putKV(key, value);
                result = "PUT_SUCCESS";
                logger.info("Successfully added key " + key + " and value " 
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
        }
    }

    private void handleGetCmd (String key) {
        //Done in KVServer;
        System.out.println("GET Key " + key);
    }
   
    public boolean onDisk(String key) {
        boolean foundOnDisk = false;

        if(Files.exists(storagePath)) {
            //return message that key found in Storage
            foundOnDisk = true;
        }
        return foundOnDisk;
    }

    public void storeKV(String key, String value) throws IOException {
        //TODO : Cache it in KVServer
 
        FileWriter file = new FileWriter(this.storagePath.toString());      
        String input = new String(Files.readAllBytes(this.storagePath), StandardCharsets.UTF_8);
        JSONObject kvStorage = (JSONObject) JSONValue.parse(input);
        kvStorage.put(key, value);

        String output = JSONValue.toJSONString(kvStorage);
        Files.write(this.storagePath, output.getBytes(StandardCharsets.UTF_8));

    }

    public String getValue(String key) throws IOException {
        //TODO: Check is value in Cache in KVServer
        String value = "";
        String input;
        
        if(Files.exists(this.storagePath)) {
            try{
//              value = new String(Files.readAllBytes(storagePath));

                input = new String(Files.readAllBytes(this.storagePath), StandardCharsets.UTF_8);
                JSONObject kvStorage = (JSONObject) JSONValue.parse(input);
//              value = kvStorage.getString(key);


            } catch (Exception ex) {
                logger.error("Unable to open file. ERROR: " + ex);
            }
        }           
        else {
            logger.info("Server: " + clientSocket.getInetAddress() + " at port: " + 
        clientSocket.getLocalPort() + "\tKey: " + key + " does not exist");     
        }       
        return value;
    }

    /**
     * Method sends a TextMessage using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream 
     */
    public void sendMessage(TextMessage msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("SEND \t<" 
                + clientSocket.getInetAddress().getHostAddress() + ":" 
                + clientSocket.getPort() + ">: '" 
                + msg.getMsg() +"'");
    }
    
    
    private TextMessage receiveMessage() throws IOException {
        
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];
        
        /* read first char from stream */
        byte read = (byte) input.read();    
        boolean reading = true;

        while(read != 10 && read !=-1 && reading) {/* CR, LF, error */
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
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }
        
        msgBytes = tmp;
        
        /* build final String */
        TextMessage msg = new TextMessage(msgBytes);
        logger.info("RECEIVE \t<" 
                + clientSocket.getInetAddress().getHostAddress() + ":" 
                + clientSocket.getPort() + ">: '" 
                + msg.getMsg().trim() + "'");
        return msg;
    }    
}



//  public boolean deleteKV(String key) throws IOException {
//      //TODO: Delete it from Cache in KVServer
//      //delete KV Pair from the persistent memory
//      boolean delete_successful = false;
//      String input = new String(Files.readAllBytes(this.storagePath), StandardCharsets.UTF_8);
//      JSONObject kvStorage = (JSONObject) JSONValue.parse(input);
//      if(kvStorage.has(key)) {
//          kvStorage.remove(key);  
//          logger.info("Server: "+ clientSocket.getInetAddress().getHostName() + " at port: " + 
//                      clientSocket.getLocalPort() + "\tDELETE SUCCESS for key: " + key);
//          delete_successful = true;
//  
//      }
//      else {
//          logger.error("Server: " + clientSocket.getInetAddress().getHostName() + " at port: " + 
//                      clientSocket.getLocalPort() + "\tKey: " + key + " does not exist");
//      }
//
//      String output = JSONValue.toJSONString(kvStorage);
//      Files.write(this.storagePath, output.getBytes(StandardCharsets.UTF_8));
//
//
//      if(Files.notExists(this.storagePath.toString())) {
//          //return message that key doesn't exist
//          logger.error("Server: " + clientSocket.getInetAddress().getHostName() + " at port: " + 
//      clientSocket.getLocalPort() + "\tKey: " + key + " does not exist");         
//      }
//      else {
//          //delete file and return message that deletion successful
//          try {
//              Files.delete(storagePath);
//              logger.info("Server: "+ clientSocket.getInetAddress().getHostName() + " at port: " + 
//      clientSocket.getLocalPort() + "\tDELETE SUCCESS for key: " + key);
//              delete_successful = true;
//
//          } catch (IOException x) {
//              logger.error("Server: "+ clientSocket.getInetAddress().getHostName() + " at port: " + 
//      clientSocket.getLocalPort() + "\tDELETE ERROR for key: " + key);            
//          }
//      }
//
//      return delete_successful;
//  }
    
