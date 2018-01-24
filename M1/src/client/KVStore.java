package client;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

import common.messages.KVMessage;
import client.ClientSocketListener.SocketStatus;

import java.net.UnknownHostException;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVStore implements KVCommInterface {
    private static Logger logger = Logger.getRootLogger();
    private Set<ClientSocketListener> listeners;
    
    private String serverAddr;
    private int serverPort;
    private boolean running;

    private Socket clientSocket;
    private OutputStream output;
    private InputStream input;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    /**
     * Initialize KVStore with address and port of KVServer
     * @param address the address of the KVServer
     * @param port the port of the KVServer
     */
    public KVStore(String address, int port) 
            throws UnknownHostException, IOException {
        // TODO Auto-generated method stub
        this.serverAddr = address;
        this.serverPort = port;
    }

    @Override
    public void connect() 
            throws UnknownHostException, IOException {
        // TODO Auto-generated method stub
        clientSocket = new Socket(this.serverAddr, this.serverPort);
        listeners = new HashSet<ClientSocketListener>();
        setRunning(true);
        logger.info("Connection established with address " + this.serverAddr + 
            " at port " + this.serverPort);
        output = clientSocket.getOutputStream();
        input = clientSocket.getInputStream();  
    }

    /**
     * Initializes and starts the client connection. 
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();
            
            /*while(isRunning()) {
                try {
                    TextMessage latestMsg = receiveMessage();
                    for(ClientSocketListener listener : listeners) {
                        listener.handleNewMessage(latestMsg);
                    }
                } catch (IOException ioe) {
                    if(isRunning()) {
                        logger.error("Connection lost!");
                        try {
                            tearDownConnection();
                            for(ClientSocketListener listener : listeners) {
                                listener.handleStatus(
                                        SocketStatus.CONNECTION_LOST);
                            }
                        } catch (IOException e) {
                            logger.error("Unable to close connection!");
                        }
                    }
                }
            }*/
        }
        catch (IOException ioe) {
            logger.error("Connection could not be established!");
            
        }
        finally {
            if(isRunning()) {
                disconnect();
            }
        }
    }

    @Override
    public synchronized void disconnect() {
        // TODO Auto-generated method stub
        logger.info("trying to close connection ...");
        
        try {
            tearDownConnection();
            for(ClientSocketListener listener : listeners) {
                listener.handleStatus(SocketStatus.DISCONNECTED);
            }
        } 
        catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }

    private void tearDownConnection() 
            throws IOException {
        setRunning(false);
        logger.info("tearing down the connection ...");
        if (clientSocket != null) {
            clientSocket.close();
            clientSocket = null;
            logger.info("connection closed!");
        }
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public KVMessage get(String key) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean run) {
        running = run;
    }

    public void addListener(ClientSocketListener listener){
        listeners.add(listener);
    }
}
