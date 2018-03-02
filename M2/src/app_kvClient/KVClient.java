package app_kvClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.net.UnknownHostException;
import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.*;
import common.messages.*;
import common.messages.KVMessage.*;

public class KVClient implements IKVClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private BufferedReader stdin;
    private KVStore clientStore = null;
    private boolean stop = false;

    private String serverAddress;
    private int serverPort;

    public void run() {
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            }
            catch (IOException e) {
                stop = true;
                printError("Client Application does not respond - Application terminated ");
                logger.error("Client Application does not respond - Application terminated ");
            }
        }
    }

    private void handleCommand (String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        switch (tokens[0]) {
            case "quit":
                stop = true;
                disconnect();
                System.out.println(PROMPT + "Application exit!");
                break;

            case "connect":
                if(tokens.length == 3) {
                    try {
                        serverAddress = tokens[1];
                        serverPort    = Integer.parseInt(tokens[2]);
                        newConnection(serverAddress, serverPort);
                    }
                    catch(NumberFormatException nfe) {
                        printError("Invalid port. Port must be a number!");
                        logger.error("Invalid port. Port must be a number!", nfe);
                    }
                    catch (UnknownHostException e) {
                        printError("Unknown Host! Unable to connect to " + serverAddress +
                            " at port " + serverPort);
                        logger.error("Unknown Host! Unable to connect to " + serverAddress +
                            " at port " + serverPort);
                    }
                    catch (IOException e) {
                        printError("Could not establish connection! Unable to connect to " + serverAddress +
                            " at port " + serverPort);
                        logger.error("Could not establish connection! Unable to connect to " + serverAddress +
                            " at port " + serverPort);
                    }
                }
                else {
                    printError("Invalid number of parameters for command \"connect\"");
                    printHelp();
                }
                break;

            case "disconnect":
                disconnect();
                break;

            case "put":
                if (tokens.length >= 2) {
                    String key = tokens[1];
                    if (clientStore != null && clientStore.isRunning()) {
                        // If value is null, msg will be an empty string
                        StringBuilder msg = new StringBuilder();
                        for (int i = 2; i < tokens.length; i++) {
                            msg.append(tokens[i]);
                            if (i != tokens.length - 1) {
                                msg.append(" ");
                            }
                        }

                        try {   
                            KVMessage reply = clientStore.put(key, msg.toString());
                            StatusType status = reply.getStatus();
                            if (status == KVMessage.StatusType.PUT_ERROR) {
                                System.out.println(PROMPT + "PUT_ERROR! Unable to put key value on server");
                                logger.error(reply.getStatusString() + " with <key, value>: <" +
                                    reply.getKey() + ", " + reply.getValue() + ">");
                            }
                            else if (status == KVMessage.StatusType.PUT_UPDATE) {
                                System.out.println(PROMPT + "PUT_UPDATE! Successfully updated key value on server");
                                logger.error(reply.getStatusString() + " with <key, value>: <" +
                                    reply.getKey() + ", " + reply.getValue() + ">");
                            }
                            else if (status == KVMessage.StatusType.PUT_SUCCESS) {
                                System.out.println(PROMPT + "PUT_SUCCESS! Successfully added key value pair on server");
                                logger.info(reply.getStatusString() + " with <key, value>: <" +
                                    reply.getKey() + ", " + reply.getValue() + ">");
                            }
                            else if (status == KVMessage.StatusType.DELETE_SUCCESS) {
                                System.out.println(PROMPT + "DELETE_SUCCESS! Successfully deleted key value pair on server");
                                logger.info(reply.getStatusString() + " with <key, value>: <" +
                                    reply.getKey() + ", " + reply.getValue() + ">");
                            }
                            else if (status == KVMessage.StatusType.DELETE_ERROR) {
                                System.out.println(PROMPT + "DELETE_ERROR! Unable to delete key value pair on server");
                                logger.error(reply.getStatusString() + " with <key, value>: <" +
                                    reply.getKey() + ", " + reply.getValue() + ">");
                            }
                            else if (status == KVMessage.StatusType.SERVER_STOPPED) {
                                System.out.println(PROMPT + "SERVER_STOPPED! Server not accepting any requests at the moment.");
                            }
                            else {
                                System.out.println(PROMPT + "Invalid Message Type from server!");
                                logger.error("Invalid Message Type from server!");
                            }
                        }
                        catch (Exception ex) {
                            printError("Put Failed. Exception: " + ex);
                            logger.error("Put Failed. Exception: " + ex);
                        }
                    }
                    else {
                        printError("Put Failed. Not connected");
                        logger.error("Put Failed. Not connected");
                    }
                }
                else {
                    printError("Invalid number of parameters for command \"put\"");
                    printHelp();
                }
                break;

            case "get":
                if (tokens.length == 2) {
                    String key = tokens[1];
                    try {
                        KVMessage reply = clientStore.get(key);
                        StatusType status = reply.getStatus();
                        if (status == KVMessage.StatusType.GET_ERROR) {
                            System.out.println(PROMPT + "GET_ERROR! Unable to get value with given key");
                            logger.error(reply.getStatusString() + " using key: " + key);
                        }
                        else if (status == KVMessage.StatusType.GET_SUCCESS) {
                            System.out.println(PROMPT + "GET_SUCCESS! Successfully retrieved value \"" + reply.getValue() + "\" with given key");
                            logger.info(reply.getStatusString() + " using key: " + key + " Value: " + reply.getValue());
                        }
                        else {
                                System.out.println(PROMPT + "Invalid Message Type from server!");
                                logger.error("Invalid Message Type from server!");
                        }
                    }
                    catch (Exception ex) {
                        printError("Get Failed. Exception" + ex);
                        logger.error("Get Failed. Exception: " + ex);   
                    }
                }
                else {
                    printError("Invalid number of parameters for command \"get\"");
                    printHelp();                   
                }
                break;

            case "logLevel":
                if (tokens.length == 2) {
                    String level = setLevel(tokens[1]);
                    if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                        printError("No valid log level!");
                        printPossibleLogLevels();
                    }
                    else {
                        System.out.println(PROMPT + "Log level changed to level " + level);
                    }
                }
                else {
                    printError("Invalid number of parameters for command \"logLevel\"");
                }
                break;

            case "help":
                printHelp(); 
                break;

            default:
                printError("Unknown command");
                printHelp();            
                break;
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t 1) Inserts a key-value pair into a server data strusture. \n");
        sb.append("\t\t\t\t 2) Updates current value if server already contains key \n");
        sb.append("\t\t\t\t 3) Deletes entry for given key if <value> equals null \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t Retrieves the value for the given key from the storage server. \n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t disconnects from the server \n");
        
        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
        
        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT 
                + "Possible log levels are:");
        System.out.println(PROMPT 
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private String setLevel(String levelString) {
        
        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void disconnect() {
        if (clientStore != null) {
            clientStore.disconnect();
            clientStore = null;
        }
    }

    @Override
    public void newConnection(String hostname, int port) 
        throws UnknownHostException, IOException {
        // TODO Auto-generated method stub
        clientStore = new KVStore(hostname, port);
        try {
            clientStore.connect();    
        }
        catch (Exception ex) {
            printError("Unable to establish connection with " + hostname + 
                " at port " + port);
            logger.error("Unable to establish connection with " + hostname + 
                " at port " + port);
        }
        clientStore.addListener(this);
    }

    @Override
    public KVCommInterface getStore() {
        // TODO Auto-generated method stub
        return this.clientStore;
    }

    @Override
    public void handleNewMessage(TextMessage msg) {
        if(!stop) {
            System.out.println(msg.getMsg());
            System.out.print(PROMPT);
        }
    }

    @Override
    public void handleStatus(SocketStatus status) {
        if(status == SocketStatus.CONNECTED) {

        } else if (status == SocketStatus.DISCONNECTED) {
            System.out.print(PROMPT);
            System.out.println("Connection terminated: " 
                    + serverAddress + " / " + serverPort);
            
        } else if (status == SocketStatus.CONNECTION_LOST) {
            System.out.println("Connection lost: " 
                    + serverAddress + " / " + serverPort);
            System.out.print(PROMPT);
        }
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " +  error);
    }

    /**
     * Main entry point for the KV server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            KVClient clientApp = new KVClient();
            clientApp.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
