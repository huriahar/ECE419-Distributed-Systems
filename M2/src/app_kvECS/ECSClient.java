package app_kvECS;
 
import java.util.Map;
import java.util.Collection;

import java.io.IOException;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import ecs.*;
import cache.*;

public class ECSClient implements IECSClient {
    private ECS ecsInstance = null;
    private BufferedReader stdin;

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECSClient> ";

    private Collection<IECSNode> nodesLaunched;
    private boolean stop = false;

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

            // init numNodes cacheSize cacheStrategy
            case "init":
                if(tokens.length == 4) {
                    try {
                        int numNodes = Integer.parseInt(tokens[1]);
                        int cacheSize = Integer.parseInt(tokens[2]);
                        String cacheStrategy = tokens[3];

                        if(numNodes <= ecsInstance.maxServers()) {
                            if (KVCache.isValidStrategy(cacheStrategy)) {
                                nodesLaunched = ecsInstance.addNodes(numNodes, cacheStrategy, cacheSize);
                                if(nodesLaunched == null) {
                                    printError("Unable to start any KVServers");
                                    logger.error("Unable to launch any KVServers");
                                }
                                else if(nodesLaunched.size() != numNodes) {
                                    printError("Launched only " + nodesLaunched.size() + " servers instead of " + numNodes);
                                    logger.error("Launched only " + nodesLaunched.size() + " servers instead of " + numNodes);
                                }
                            }
                            else {
                                printError("Enter a valid cache strategy. Entered value: " + cacheStrategy);
                                logger.error("Enter a valid cache strategy. Entered value: " + cacheStrategy);
                            }
                        }
                        else {
                           printError("Not enough KVServers available");
                           logger.error("Not enough KVServers available");
                        }
                    }
                    catch(NumberFormatException nfe) {
                        printError("Invalid numNodes/cacheSize. numNodes/cacheSize  must be a number!");
                        logger.error("Invalid numNodes/cacheSize. numNodes/cacheSize must be a number!", nfe);
                    }
                }
                else {
                    printError("Invalid number of parameters for command \"init\".");
                    printHelp();
                }
                break;
            
            case "start":
                for (IECSNode node : nodesLaunched) {
                    if (!ecsInstance.start(node)) {
                        printError("Unable to start KVServer: "+ node.getNodeName() + " Host: " + node.getNodeHost()+ " Port: " + node.getNodePort());
                        logger.error("Unable to start KVServer: "+ node.getNodeName() + " Host: " + node.getNodeHost()+ " Port: " + node.getNodePort());
                    }
                }
                break;

            case "stop":
                for (IECSNode node : nodesLaunched) {
                    if (!ecsInstance.stop(node)) {
                        printError("Unable to stop KVServer: "+ node.getNodeName() + " Host: " + node.getNodeHost()+ " Port: " + node.getNodePort());
                        logger.error("Unable to stop KVServer: "+ node.getNodeName() + " Host: " + node.getNodeHost()+ " Port: " + node.getNodePort());
                    }
                }
                break;

            case "shutdown":
                for (IECSNode node : nodesLaunched) {
                    if (!ecsInstance.stop(node)) {
                        printError("Unable to shutdown KVServer: "+ node.getNodeName() + " Host: " + node.getNodeHost()+ " Port: " + node.getNodePort());
                        logger.error("Unable to shutdown KVServer: "+ node.getNodeName() + " Host: " + node.getNodeHost()+ " Port: " + node.getNodePort());
                    }
                }      
                if(!ecsInstance.shutdown()) {
                    printError("Unable to exit ECS process");
                }
                break;

            // addNode cacheSize cacheStrategy
            case "addNode":
                if(tokens.length == 3) {
                    try {
                        int cacheSize = Integer.parseInt(tokens[1]);
                        String cacheStrategy = tokens[2];
                        if (KVCache.isValidStrategy(cacheStrategy)) {
                            IECSNode newNode = addNode(cacheStrategy, cacheSize);
                            if (newNode != null) {
                                nodesLaunched.add(newNode);
                            }
                            else {
                                printError("Unable to find any remaining unlaunched nodes supplied in config file");
                                logger.error("Unable to find any remaining unlaunched nodes supplied in config file");
                            }
                        }
                        else {
                            printError("Enter a valid cache strategy. Entered value: " + cacheStrategy);
                            logger.error("Enter a valid cache strategy. Entered value: " + cacheStrategy);
                        }
                    }
                    catch(NumberFormatException nfe) {
                        printError("Invalid cacheSize. cacheSize must be a number!");
                        logger.error("Invalid cacheSize. cacheSize must be a number!", nfe);
                    }
                }
                else {
                    printError("Invalid number of parameters for command \"addNode\"");
                    printHelp();
                }
                break;

            case "removeNode":
                if(tokens.length >= 2) {
                    Collection<String> nodeNames = null;
                    for (int i = 1; i < tokens.length; ++i) {
                        nodeNames.add(tokens[i]);
                    }
                    if(!ecsInstance.removeNodes(nodeNames)) {
                        printError("Unable to remove node(s): ");
                        logger.error("Unable to remove node(s): ");
                        for (String node : nodeNames) {
                            printError(node + " ");
                            logger.error(node + " ");
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
        sb.append(PROMPT).append("init <numNodes> <cacheSize> <cacheStrategy>");
        sb.append("\t Launches numNodes servers with cacheSize and cacheStrategy\n");
        sb.append(PROMPT).append("start");
        sb.append("\t 1) Starts storage service on all launched server instances \n");
        sb.append(PROMPT).append("stop");
        sb.append("\t\t Stops the storage service (get/put requests) but process is running. \n");
        sb.append(PROMPT).append("shutdown");
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
