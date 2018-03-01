package ecs;
//import common.md5;
import common.ServerMetaData;
import common.ServerMetaData.ConfigContent;
import app_kvServer.KVServer;
import common.KVConstants.*;
import ecs.IECSNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.math.BigInteger;
import java.util.Collection;

public class ECS {
	private String confPath; 
    private HashMap<BigInteger, ServerMetaData> ringNetwork;
    

	public ECS(String confFile)
    {
        this.configFile = Paths.get(configFile);
        this.ringNetwork = new HashMap<BigInteger, ServerMetaData>();
        populateRingNetwork();
    }


    private void populateRingNetwork() throws IOException {
        ArrayList<String> lines = new ArrayList<>(Files.readAllLines(this.configFile, standardCharsets.UTF_8));
        int numServers = Math.floor(lines.size()/2);
        
        for(int i = 0; i < numServers ; i++) {
            String[] line = lines.get(i).split(" ");
            BigInteger serverHash = md5.encode(line[SERVER_NAME] + DELIM + line[SERVER_IP] + DELIM + line[SERVER_PORT]);
            //TODO figure out begin and end hash, does zookeeper assign it? No it doesn't. Have to do it ourselves
            ServerMetaData meta = new ServerMetaData(line[SERVER_NAME], line[SERVER_IP], Integer.parseInt(line[SERVER_PORT]), null, null);
            this.ringNetwork.put(serverHash, meta);
            
        }

    }

    

    @Override
    public boolean start() {
        // TODO
        boolean failed = false;
        //Loop through the list of nodes in Collections and change their status away from STOPPED   
        for (iterator<IECSNodes> iter = nodesLaunched.iterator(); iter.hasNext();) {
           // if(!ecsInstance.start(iter)) {
           //     failed = true;
           // } 
            
                      
        }
        return failed;
    }

    @Override
    public boolean stop() {
        // TODO
        //Loop through the list of ECS
        for (iterator<IECSNodes> iter = nodesLaunched.iterator(); iter.hasNext();) {
            if(!ecsInstance.stop(iter)) {
                failed = true;
            }               
        }
        return failed;

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
        //Iterator<Integer> iter = l.iterator();
        //while (iter.hasNext()) {
        //    if (iter.next().intValue() == 5) {
        //        iter.remove();
        //    }
        //}

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

}
