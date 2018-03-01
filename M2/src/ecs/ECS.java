package ecs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.math.BigInteger;
import java.util.Collection;

import org.apache.log4j.Logger;

import common.*;

public class ECS implements IECS {
	private Path configFile; 
    private HashMap<BigInteger, ServerMetaData> ringNetwork;
    private static Logger logger = Logger.getRootLogger();

	public ECS(String configFile) {
        this.configFile = Paths.get(configFile);
        this.ringNetwork = new HashMap<BigInteger, ServerMetaData>();
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

    @Override
    public boolean start(IECSNode server) {
        // TODO
        boolean failed = false;
        //Loop through the list of nodes in Collections and change their status away from STOPPED   
        return failed;
    }

    @Override
    public boolean stop(IECSNode server) {
        // TODO
        //Loop through the list of ECS
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
