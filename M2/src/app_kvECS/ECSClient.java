package app_kvECS;
 
import java.util.Map;
import java.util.Collection;
import java.util.ArrayList;

import java.io.IOException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import common.KVConstants;

import ecs.*;
import cache.*;

public class ECSClient implements IECSClient {
    private ECS ecsInstance = null;
    private BufferedReader stdin;

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECSClient> ";

    private Collection<IECSNode> nodesLaunched;
    private boolean stop = false;
    private int timeout = KVConstants.LAUNCH_TIMEOUT;

    public ECSClient (String configFile) {
    	Path ecsConfig = Paths.get(configFile);
    	// If file doesn't exist or does not point to a valid file
    	if (!Files.exists(ecsConfig)) {
    		printError("ECS Config file does not exist! " + ecsConfig);
            logger.error("ECS Config file does not exist!");
            System.exit(1);
    	}
    	else {
            this.ecsInstance = new ECS(ecsConfig);
    	} 
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
                        String cacheStrategy = tokens[2];
                        int cacheSize = Integer.parseInt(tokens[3]);
                        System.out.println(numNodes + " " + cacheSize + " " + cacheStrategy);

                        if(numNodes <= ecsInstance.availableServers()) {
                            if (KVCache.isValidStrategy(cacheStrategy)) {
                                nodesLaunched = addNodes(numNodes, cacheStrategy, cacheSize);
                                if(nodesLaunched == null) {
                                    printError(PROMPT + "Unable to start any KVServers");
                                    logger.error("Unable to launch any KVServers");
                                }
                                else if(nodesLaunched.size() != numNodes) {
                                    printError(PROMPT + "Launched only " + nodesLaunched.size() + " servers instead of " + numNodes);
                                    logger.error("Launched only " + nodesLaunched.size() + " servers instead of " + numNodes);
                                }
                            }
                            else {
                                printError(PROMPT + "Enter a valid cache strategy. Entered value: " + cacheStrategy);
                                logger.error("Enter a valid cache strategy. Entered value: " + cacheStrategy);
                            }
                        }
                        else {
                           printError(PROMPT + "Not enough KVServers available");
                           logger.error("Not enough KVServers available");
                        }
                    }
                    catch(NumberFormatException nfe) {
                        printError(PROMPT + "Invalid numNodes/cacheSize. Must be a number!");
                        logger.error("Invalid numNodes/cacheSize. Must be a number!", nfe);
                    }
                }
                else {
                    printError(PROMPT + "Invalid number of arguments");
                    printHelp();
                }
                break;
            
            case "start":
                if(!start()) {
                    printError(PROMPT + "Start failed");
                    logger.error("Start failed");
                }
                break;

            case "stop":
                if(!stop()) {
                    printError(PROMPT + "Stop failed");
                    logger.error("Stop failed");
                }
                break;

            case "shutdown":
                if(!shutdown()) {
                    printError(PROMPT + "Unable to exit ECS process");
                    logger.error("shutDown failed");
                }
                stop = true;
                disconnect();
                System.out.println(PROMPT + "Application exit!");
                break;

            // addNode cacheSize cacheStrategy
            case "addNode":
                if(tokens.length == 3) {
                    try {
                        String cacheStrategy = tokens[1];
                        int cacheSize = Integer.parseInt(tokens[2]);
                        if (KVCache.isValidStrategy(cacheStrategy)) {
                            IECSNode newNode = addNode(cacheStrategy, cacheSize);
                            if (newNode != null) {
                                nodesLaunched.add(newNode);
                            }
                            else {
                                printError(PROMPT + "Unable to find any remaining unlaunched nodes supplied in config file");
                                logger.error("Unable to find any remaining unlaunched nodes supplied in config file");
                            }
                        }
                        else {
                            printError(PROMPT + "Enter a valid cache strategy. Entered value: " + cacheStrategy);
                            logger.error("Enter a valid cache strategy. Entered value: " + cacheStrategy);
                        }
                    }
                    catch(NumberFormatException nfe) {
                        printError(PROMPT + "Invalid cacheSize. cacheSize must be a number!");
                        logger.error("Invalid cacheSize. cacheSize must be a number!", nfe);
                    }
                }
                else {
                    printError(PROMPT + "Invalid number of parameters for command \"addNode\"");
                    printHelp();
                }
                break;

            case "removeNode":
                if(tokens.length >= 2) {
                    Collection<String> nodeNames = new ArrayList<String>();
                    for (int i = 1; i < tokens.length; ++i) {
                        nodeNames.add(tokens[i]);
                    }
                    if(!removeNodes(nodeNames)) {
                        printError(PROMPT + "Unable to remove node(s): ");
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
                        printError(PROMPT + "No valid log level!");
                        printPossibleLogLevels();
                    }
                    else {
                        System.out.println(PROMPT + "Log level changed to level " + level);
                    }
                }
                else {
                    printError(PROMPT + "Invalid number of parameters for command \"logLevel\"");
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
        sb.append(PROMPT).append("init <numNodes> <cacheStrategy> <cacheSize>");
        sb.append("\t Launches numNodes servers with cacheSize and cacheStrategy\n");
        sb.append(PROMPT).append("start");
        sb.append("\t 1) Starts storage service on all launched server instances \n");
        sb.append(PROMPT).append("stop");
        sb.append("\t\t Stops the storage service (get/put requests) but process is running. \n");
        sb.append(PROMPT).append("shutdown");
        sb.append("\t\t Stop all the servers and shuts down process  \n");
        sb.append(PROMPT).append("addNode <cacheStrategy> <cacheSize>");
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
        ecsInstance.disconnect();    
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    public boolean start() {
        boolean success = true; 
        for (IECSNode node : nodesLaunched) {
            if (!ecsInstance.start(node)) {
                success = false;
                printError(PROMPT + "Unable to start KVServer: "+ node.getNodeName() + " Host: " + node.getNodeHost()+ " Port: " + node.getNodePort());
                logger.error("Unable to start KVServer: "+ node.getNodeName() + " Host: " + node.getNodeHost()+ " Port: " + node.getNodePort());
            }
        }
        return success; 
    }

    @Override
    public boolean stop() {
        boolean success = true;

        for (IECSNode node : nodesLaunched) {
            if (!ecsInstance.stop(node)) {
                success = false;
                printError(PROMPT + "Unable to stop KVServer: "+ node.getNodeName() + " Host: " + node.getNodeHost()+ " Port: " + node.getNodePort());
                logger.error("Unable to stop KVServer: "+ node.getNodeName() + " Host: " + node.getNodeHost()+ " Port: " + node.getNodePort());
            }
        }
        return success;
    }


    @Override
    public boolean shutdown() {
        boolean success = true;
        success = ecsInstance.removeNodes(nodesLaunched);
        if(success) nodesLaunched.clear();
        success = success & ecsInstance.shutdown();
        return success;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        if(ecsInstance.ringNetworkSize() == 0) {
            return setupFirstNode(cacheStrategy, cacheSize);
        }
        return ecsInstance.addNode(cacheStrategy, cacheSize);
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        return setupNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        //First init and launch the server that has all the data - aka the lastRemoved
        IECSNode firstNode = setupFirstNode(cacheStrategy, cacheSize);
        //Then init and launch the remaining count - 1servers if there are any
        Collection<IECSNode> nodes = new ArrayList<IECSNode>();
        nodes.add(firstNode);
        count--;
        while(count > 0) {
            nodes.add(addNode(cacheStrategy, cacheSize));
            count--;
        }
        this.nodesLaunched = nodes;
        //Await nodes queries ZK nodes for a status update
        try {
			awaitNodes(count, KVConstants.LAUNCH_TIMEOUT);
		} catch (Exception e) {
			e.printStackTrace();
		}
        return nodes;
    }

    public IECSNode setupFirstNode(String cacheStrategy, int cacheSize) {
        //This function should only be used to setup the first node on the ring network
        boolean success = false;
    	// This is called before launching the servers - decides which nodes to add in hashRing, adds them
        //TODO this loop only iterates once... always... remove loop
    	Collection<IECSNode> nodes  = ecsInstance.initAddNodesToHashRing();
        if(nodes.size() == 0) {
            logger.error("failed to init first node in ringNetwork!");
            return null;
        }
    	for(IECSNode entry: nodes) {
            //for each node, launch server and set status as stopped
            if(ecsInstance.launchKVServer(entry, cacheStrategy, cacheSize)) {
                logger.info("SUCCESS. Launched KVServer :" + entry.getNodeName());
            } 
            else {
                logger.error("ERROR. Unable to launch KVServer :" + entry.getNodeName() + " Host: " + entry.getNodeHost()+ " Port: " + entry.getNodePort());           
            }
            logger.error("entry name is " + entry.getNodeName());
        }
        success = ecsInstance.alertMetaDataUpdate();
        if(success)
        	nodes = ecsInstance.setupNodesCacheConfig(nodes, cacheStrategy, cacheSize);
        else {
        	logger.error("Unable to do meta data update for servers");
        }
        return nodes.iterator().next();    
    }


    /**
     * Wait for all nodes to report status or until timeout expires
     * @param count     number of nodes to wait for
     * @param timeout   the timeout in milliseconds
     * @return  true if all nodes reported successfully, false otherwise
     */
    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        String path = null, status = null;
        int counter = 0;
        long endTimeMillis = System.currentTimeMillis() + timeout;
        for(IECSNode entry : nodesLaunched) {
            path = ecsInstance.getZKPath(entry.getNodeName());
            if(path != null) {
                status = ecsInstance.checkZKStatus(path,endTimeMillis); 
                if(System.currentTimeMillis() > endTimeMillis) {
                    return false;
                }   
                if(status.equals("SERVER_LAUNCHED")) {
                    counter++;                
                }
            }
        }
        return (counter == count);
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        Collection<IECSNode> nodesToRemove = new ArrayList<IECSNode>();
        boolean success = true;
        for(String name : nodeNames) {
            for(IECSNode entry: nodesLaunched) {
                if(entry.getNodeName().equals(name))
                    nodesToRemove.add(entry);
            }
        }
        if(ecsInstance.removeNodes(nodesToRemove)) {
            nodesLaunched.removeAll(nodesToRemove);
        }
        else 
            success = false;
            
        return success;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        return ecsInstance.getNodes();
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return ecsInstance.getNodeByKey(Key);
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/ECSClient.log", Level.ALL);
            if (args.length != 1) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: ecs <ecs.config>");
            }
            else {
                String configFile = args[0];
                System.out.println(System.getProperty("user.dir"));
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
