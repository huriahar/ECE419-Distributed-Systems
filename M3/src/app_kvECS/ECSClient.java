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

    public ECSClient(String zkHostname, int zkPort) {
        this("ecs.config", "localhost");
        this.ecsInstance.setZKInfo(zkHostname, zkPort);
    }

    public ECSClient (String configFile, String ugmachine) {
    	Path ecsConfig = Paths.get(configFile);
        this.nodesLaunched = new ArrayList<IECSNode>();
    	// If file doesn't exist or does not point to a valid file
    	if (!Files.exists(ecsConfig)) {
    		printError("ECS Config file does not exist! " + ecsConfig);
            logger.error("ECS Config file does not exist!");
            System.exit(1);
    	}
    	else {
            this.ecsInstance = new ECS(ecsConfig, ugmachine);
    	} 
    }

    public void run() {
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                boolean nocrashes = this.checkServerStatus();
                if(nocrashes) {
                    String cmdLine = stdin.readLine();
                    this.handleCommand(cmdLine);
                }
            }
            catch (IOException e) {
                stop = true;
                printError("ECSlient Application does not respond - Application terminated ");
                logger.error("ESClient Application does not respond - Application terminated ");
            }
        }
    }

    public boolean checkServerStatus() {
        boolean nocrashes = true;
        Collection<IECSNode> nodesToRemove = new ArrayList<IECSNode>();
        for (IECSNode node : nodesLaunched) {
            boolean success = ecsInstance.checkServersStatus(node);
            if(!success) {
                logger.error("Removing " + node.getNodeName() + " from system");
                nodesToRemove.add(node);
                nocrashes = false;
            }
        }
        //true indicates that this is a list of crashed servers
        removeNodesDirectly(nodesToRemove, true);
        //TODO addNodes
        return nocrashes;
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
            case "unlock":
                if(tokens.length == 2) {
                    if(!unlock(tokens[1])) {
                        printError(PROMPT + "Write unlock " + tokens[1] + " failed.");
                        logger.error("Write unlock failed");
                    }
                }
                else {
                    printError(PROMPT + "Invalid number of arguments");
                    printHelp();
                }
                break;
            case "lock":
                if(tokens.length == 2) {
                    if(!lock(tokens[1])) {
                        printError(PROMPT + "Write lock " + tokens[1] + " failed.");
                        logger.error("Write lock failed");
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
                            if (newNode == null) {
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
        sb.append("\t\t Stops the storage service (get/put requests) but process is running.\n");
        sb.append(PROMPT).append("lock <nodeName>");
        sb.append("\t\t Write locks the storage service (locks put requests) but gets are acceptable.\n");
        sb.append(PROMPT).append("unlock <nodeName>");
        sb.append("\t\t Write unlocks the storage service (unlocks put requests).\n");
        sb.append(PROMPT).append("shutdown");
        sb.append("\t\t Stop all the servers and shuts down process\n");
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

    public String setLevel(String levelString) {
        
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

    public void disconnect() {
        ecsInstance.disconnect();    
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    public boolean unlock(String name) {
        for (IECSNode node : nodesLaunched) {
            if(node.getNodeName().equals(name)) {
                return ecsInstance.sendWriteUnlock(node);
            }
        }
        return false;
    }

    public boolean lock(String name) {
        for (IECSNode node : nodesLaunched) {
            if(node.getNodeName().equals(name)) {
                return ecsInstance.sendWriteLock(node);
            }
        }
        return false;
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
        success = removeNodesDirectly(nodesLaunched, false);
        success = success & ecsInstance.shutdown();
        return success;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        if(ecsInstance.availableServersCount() == 0) return null;
        IECSNode node = null;
        if(ecsInstance.ringNetworkSize() == 0) {
            node = setupFirstNode(cacheStrategy, cacheSize);
            //If we couldn't find the lastRemoved... use any available
            //server
            if(ecsInstance.ringNetworkSize() == 0) {
                node = ecsInstance.addNode(cacheStrategy, cacheSize);
            }
        }
        else {
            node = ecsInstance.addNode(cacheStrategy, cacheSize);
        }
        if(node != null) {
            this.nodesLaunched.add(node);
        } else {
        System.out.println("node is null");
        }
        return node;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        if(count > ecsInstance.availableServersCount()) return new ArrayList<IECSNode>();
        return setupNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        Collection<IECSNode> nodes = new ArrayList<IECSNode>();
        int counter = 0;
        while(counter < count) {
            nodes.add(addNode(cacheStrategy, cacheSize));
            counter++;
        }
        //Await nodes queries ZK nodes for a status update
        try {
			if(!awaitNodes(count, KVConstants.LAUNCH_TIMEOUT)) {
                logger.error("awaitNodes times out! servers are not responsive");
            }
		} catch (Exception e) {
			e.printStackTrace();
		}
        return nodes;
    }

    public IECSNode setupFirstNode(String cacheStrategy, int cacheSize) {
        //This function should only be used to setup the first node on the ring network
        boolean success = false;
    	// This is called before launching the servers - decides which nodes to add in hashRing, adds them
    	IECSNode node = ecsInstance.initAddNodesToHashRing();
        if(node == null) {
            logger.debug("Could not find lastRemoved in the list of available servers!");
            logger.debug("Will look for any available server..");
            return null;
        }
        //for each node, launch server and set status as stopped
        if(ecsInstance.launchKVServer(node, cacheStrategy, cacheSize)) {
            logger.info("SUCCESS. Launched KVServer :" + node.getNodeName());
        } 
        else {
            logger.error("ERROR. Unable to launch KVServer :" + node.getNodeName() + " Host: " + node.getNodeHost()+ " Port: " + node.getNodePort());           
        }
        //Update meta data and setup cache config for node
        success = ecsInstance.sendMetaDataUpdate(node);
        success = success & ecsInstance.setupNodesCacheConfigOneNode(node, cacheStrategy, cacheSize);
        if(!success) {
        	logger.error("Unable to do meta data update for servers");
        }
        return node;
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
        long endTimeMillis = System.currentTimeMillis() + timeout*4;
        for(IECSNode entry : nodesLaunched) {
            path = ecsInstance.getZKPath(entry.getNodeName());
            if(path != null) {
                status = ecsInstance.checkZKStatus(path,endTimeMillis); 
                if(status.equals("SERVER_LAUNCHED")) {
                    counter++;                
                }
                if(System.currentTimeMillis() > endTimeMillis) {
                    return false;
                }   
            }
        }
        return (counter == count);
    }

    
    public boolean removeNodesDirectly(Collection<IECSNode> nodes, boolean nodesCrashed) {
        //Use this function if you want to remove nodes given a list of
        //nodes (as opposed to a list of node names)
        if(ecsInstance.removeNodes(nodes, nodesCrashed)) {
            nodesLaunched.removeAll(nodes);
            return true;
        }
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        Collection<IECSNode> nodesToRemove = new ArrayList<IECSNode>();
        for(String name : nodeNames) {
            for(IECSNode entry: nodesLaunched) {
                if(entry.getNodeName().equals(name))
                    nodesToRemove.add(entry);
            }
        }
        return removeNodesDirectly(nodesToRemove, false);
    }

    public void addToLaunchedNodes(IECSNode node) {
        nodesLaunched.add(node);
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
            if (args.length != 2) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: ecs <ecs.config> <ugXXX>");
            }
            else {
                String configFile = args[0];
                System.out.println(System.getProperty("user.dir"));
                ECSClient ECSClientApp = new ECSClient(configFile, args[1]);
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
