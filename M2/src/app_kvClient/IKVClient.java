package app_kvClient;

import client.KVCommInterface;
import common.messages.TextMessage;

public interface IKVClient {

    public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};

    /**
     * Reads and processes commands entered by the client on terminal prompt
     */
    public void run();

    /**
     * Creates a new connection to hostname:port
     * @throws Exception
     *      when a connection to the server can not be established
     */
    public void newConnection(String hostname, int port) throws Exception;

    /**
     * Get the current instance of the Store object
     * @return  instance of KVCommInterface
     */
    public KVCommInterface getStore();

    public void handleNewMessage(TextMessage msg);
    
    public void handleStatus(SocketStatus status);
}
