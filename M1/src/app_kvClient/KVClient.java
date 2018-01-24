package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVClient implements IKVClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private BufferedReader stdin;

    private boolean stop = false;

    private KVStore clientStore = null;
    private String serverAddress;
    private int serverPort;

    public void run() {
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
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
                    } catch(NumberFormatException nfe) {
                        printError("No valid address. Port must be a number!");
                        logger.error("Unable to parse argument <port>", nfe);
                    } catch (UnknownHostException e) {
                        printError("Unknown Host!");
                        logger.error("Unknown Host!", e);
                    } catch (IOException e) {
                        printError("Could not establish connection!");
                        logger.error("Could not establish connection!", e);
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
                if(tokens.length >= 3){
                break;

            case "get":
                if(tokens.length == 2){
                    ;
                }
                else {
                    ;
                }
                break;


            case "logLevel":
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
        sb.append(PROMPT).append("send <text message>");
        sb.append("\t\t sends a text message to the server \n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");
        
        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
        
        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void disconnect() {
        if(clientStore != null) {
            clientStore.disconnect();
            clientStore = null;
        }
    }


    private void sendMessage(String msg){
        try {
            client.sendMessage(new TextMessage(msg));
        } catch (IOException e) {
            printError("Unable to send message!");
            disconnect();
        }
    }

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub
        clientStore = new KVStore(hostname, port);
        try {
            clientStore.connect();
        }
        catch (Exception ex) {
            printError("Connection to " + hostname + " at port " + port + 
                " could not be established.");
            logger.error("Connection to " + hostname + " at port " + port + 
                " could not be established. Error: " + ex);           
        }
    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return this.clientStore;
    }

    private void printError(String error){
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
