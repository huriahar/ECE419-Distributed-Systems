package app_kvECS;
 
import java.util.Map;
import java.util.Collection;
import java.util.Iterator;

import java.io.IOException;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import ecs.*;

public class ECSClient implements IECSClient {
    private ECS ecsInstance;
    private BufferedReader stdin;
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECSClient> ";
    private Collection<IECSNode> nodesLaunched;
    private boolean stop = false;
    private ECS ecsInstance = null;

    public ECSClient (String configFile) {
        this.ecsInstance = new ECS(configFile); 
    }


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
                printError("ECSlient Application does not respond - Application terminated ");
                logger.error("ESClient Application does not respond - Application terminated ");
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

            case "init":
                if(tokens.length == 4) {
                    try {
                        int numNodes     = Integer.parseInt(tokens[1]);
                        int cacheSize   = Integer.parseInt(tokens[2]);
                        String cacheStrategy = tokens[3];
                        if(numNodes <= ecsInstance.ringNetworks.size()) { 
                            nodesLaunched = ecsInstance.addNodes(numNodes, cacheStrategy, cacheSize);
                            if(nodesLaunched == null) {
                                printError("Unable to start any KVServers");
                            }
                            else if(nodesLaunched.size() != numNodes) {
                                printError("Unable to start all KVServers");
                            }           
                        }
                        else {
                           printError("Enough KVServers not available");
                        }
                    }
                    catch(NumberFormatException nfe) {
                        printError("Invalid numNodes/cacheSize. numNodes/cacheSize  must be a number!");
                        logger.error("Invalid numNodes/cacheSize. numNodes/cacheSize must be a number!", nfe);
                    }
                }
                else {
                    printError("Invalid number of parameters for command \"connect\"");
                    printHelp();
                }
                break;
            
            case "start":
                for (int i = 0; i < nodesLaunched.size(); i++) {
                    if(!ecsInstance.start(nodesLaunched.get(i))) {
                        printError("Unable to start KVServer: "+ iter.getNodeName() + " Host: " + iter.getNodeHost()+ " Port: " + iter.getNodePort()) ;
                    } 
                }       
                break;

            case "stop":
                for(int i = 0; i < nodesLaunched.size(); i++) {
                    if(!ecsInstance.stop(nodesLaunched.get(i))) {
                        printError("Unable to stop KVServer: "+ iter.getNodeName() + " Host: " + iter.getNodeHost()+ " Port: " + iter.getNodePort()) ;
                    } 
                }       
                break;

            case "shutDown": 
                for(int i = 0; i < nodesLaunched.size(); i++) {     
                    if(!ecsInstance.stop(nodesLaunched.get(i))) {
                        printError("Unable to stop KVServer: "+ iter.getNodeName() + " Host: " + iter.getNodeHost()+ " Port: " + iter.getNodePort()) ;
                    } 
                }      
                if(!ecsInstance.shutDown()) {
                    printError("Unable to exit process");
                }
                break;

            case "addNode":
                if(tokens.length == 3) {
                    try {
                        int cacheSize = Integer.parseInt(tokens[1]);
                        String cacheStrategy = tokens[2];
                        IECSNode newNode = addNode(cacheStrategy, cacheSize);
                        if(newNode == null) {
                            printError("Unable to find Node containing Key: " + Key);    
                        }
                        else 
                            nodesLaunched.add(newNode);
                    }
                    catch(NumberFormatException nfe) {
                        printError("Invalid cacheSize. cacheSize must be a number!");
                        logger.error("Invalid cacheSize. cacheSize must be a number!", nfe);
                    }

                }
                else {
                    printError("Invalid number of parameters for command \"connect\"");
                    printHelp();
                }
                break;

            case "removeNode":
                if(tokens.length() >= 2) {
                    Collections<String> nodeNames;
                    for(int i = 1; i < tokens.length(); i++) {
                        nodeNames.add(tokens[i]);
                    }    
                    if(!ecsInstance.removeNodes(nodeNames)) {
                        printError("Unable to remove node(s): ");   
                        for(int i = 0; i < nodeNames.size(); i++) {
                            printError(nodeNames[i]+ " ");
                        }    
                    }
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
        sb.append(PROMPT).append("ECS CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("addNodes <numNodes> <cacheSize> <cacheStrategy>");
        sb.append("\t Launches numNodes servers with cacheSize and cacheStrategy\n");
        sb.append(PROMPT).append("start");
        sb.append("\t 1) Starts storage service on all launched server instances \n");
        sb.append(PROMPT).append("stop");
        sb.append("\t\t Stops the storage servive but process is running. \n");
        sb.append(PROMPT).append("shutDown");
        sb.append("\t\t Stop all the servers and shuts down process  \n");
        sb.append(PROMPT).append("addNode <cacheSize> <cacheStrategy>");
        sb.append("\t\t Adds a new server of cacheSize with cacheStrategy \n");
        sb.append(PROMPT).append("removeNode <serverName1> <serverName2> ...");
        sb.append("\t\t Removes server with given names \n");
       
 
        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
        
        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
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
        
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    public boolean start() {
        // TODO
        return false; 
    }

    @Override
    public boolean stop() {
        // TODO
        return false;

    }


    @Override
    public boolean shutdown() {
        // TODO
            
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
         
        return null; 
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO

        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    public static void main(String[] args) {
        // TODO
        try {
            new LogSetup("logs/ECSClient.log", Level.ALL);
            if (args.length != 1) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: ecs <ecs.config>");
            }
            else {
                System.out.println("Config file " + args[0]);
                String configFile = args[0];
                ECSClient ECSClientApp = new ECSClient(configFile);
                ECSClientApp.run();
            }
        }
        catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }



}
