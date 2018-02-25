import java.util.Map;
import java.util.Collection;

import java.io.IOException;

import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import ecs.IECSNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;




public class ECSClient implements IECSClient {

	private ECS ecsInstance;
    private BufferedReader stdin;
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECSClient> ";
	Collection<IECSNodes> nodesLaunched;


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
                        int numNodes 	= Integer.parseInt(tokens[1]);
                        int cacheSize   = Integer.parseInt(tokens[2]);
						String cacheStrategy = tokens[3];
                        nodesLaunched = addNodes(numNodes, cacheSize, cacheStrategy);	
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

            case "stop":
                if(!stop()) {
					printError("Unable to stop the service");	
				}
                break;

            case "shutDown": 
				if(!shutDown()) {
					printError("Unable to stop and exit");
				}	
				break;

			case "addNode":
				if(tokens.length == 3) {
					try {
						int cacheSize = Integer.parseInt(tokens[1]);
						String cacheStrategy = tokens[2];
						IECS newNode = addNode(cacheSize, cacheStrategy);	
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
					if(!removeNodes(nodeNames)) {
						printError("Unable to remove node");	
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
                break
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


    @Override
    public boolean start() {
        // TODO
		//Loop through the list of nodes in Collections and change their status away from STOPPED	
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
		//Loop through the list of ECS
        return false;
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
        return 
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
		//Iterator<Integer> iter = l.iterator();
		//while (iter.hasNext()) {
		//    if (iter.next().intValue() == 5) {
		//        iter.remove();
		//    }
		//}
I
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
