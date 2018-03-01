package ecs;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;

import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.math.BigInteger;
import java.util.Collection;

import org.apache.log4j.Logger;
import java.lang.Process;
//import org.apache.zookeeper.ZooKeeper;

import common.*;

public class ECS {
    private Path configFile; 
    private TreeMap<BigInteger, ServerMetaData> ringNetwork;
    private static Logger logger = Logger.getRootLogger();
//    private ZooKeeper zk;
    


	public ECS(String configFile) {
        this.configFile = Paths.get(configFile);
        this.ringNetwork = new TreeMap<BigInteger, ServerMetaData>();
        try {
			populateRingNetwork();
		} catch (IOException e) {
			logger.error("Unable to populate ring network. Exception " + e);
		}
    }

    private void populateRingNetwork()
    		throws IOException {
        ArrayList<String> lines = new ArrayList<>(Files.readAllLines(this.configFile, StandardCharsets.UTF_8));
        int numServers = (int) Math.floor(lines.size()/2);		// Why??????
        
        for (int i = 0; i < numServers ; ++i) {
            String[] line = lines.get(i).split(" ");
            BigInteger serverHash = md5.encode(line[0] + KVConstants.DELIM + line[1] + KVConstants.DELIM + line[2]);
            //TODO figure out begin and end hash, does zookeeper assign it? No it doesn't. Have to do it ourselves
            ServerMetaData meta = new ServerMetaData(line[0], line[1], Integer.parseInt(line[2]), null, null);
            this.ringNetwork.put(serverHash, meta);
        }
    }

    public int maxServers() {
    	return ringNetwork.size();
    }

    public boolean start(IECSNode server) {
        // TODO
        boolean failed = false;
        //Loop through the list of nodes in Collections and change their status away from STOPPED   
        return failed;
    }

    public boolean stop(IECSNode server) {
        // TODO
        //Loop through the list of ECS
        return false;

    }


    public boolean shutdown() {
        // TODO
            
        return false;
    }

    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    /**
     * Randomly choose <numberOfNodes> servers from the available machines and start the KVServer by issuing an SSH call to the respective machine.
     * This call launches the storage server with the specified cache size and replacement strategy. For simplicity, locate the KVServer.jar in the
     * same directory as the ECS. All storage servers are initialized with the metadata and any persisted data, and remain in state stopped.
     * NOTE: Must call setupNodes before the SSH calls to start the servers and must call awaitNodes before returning
     * @return  set of strings containing the names of the nodes
     */
    public boolean initService(Collection<IECSNode> nodes, String cacheStrategy, int cacheSize) {
        // TODO
    
        Process proc;
        String script = "script.sh";
        boolean success = true;

        for(IECSNode node: nodes) {
            //SSHCall for each server and launch with the right size and strategy
            Runtime run = Runtime.getRuntime();
            String nodeHost = node.getNodeHost();
            String nodePort = Integer.toString(node.getNodePort());
            String cmd = "script " + nodeHost + " " + nodePort;
            try {
              proc = run.exec(cmd);
            } catch (IOException e) {
              e.printStackTrace();
            }
        }

        return success; 
    }

    public Collection<IECSNode> connectToZk(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    public IECSNode getNodeByKey(String Key) {
        // TODO
        BigInteger serverHash = md5.encode(Key);
        ServerMetaData metaData = ringNetwork.get(serverHash);
        IECSNode serverNode = new ECSNode(metaData.name, metaData.addr, metaData.port, metaData.bHash, metaData.eHash);   
        return serverNode;
    }


}
