package ecs;
import common.MD5;
import common.ServerMetaData;
import common.ServerMetaData.configContent;
import app_kvServer.KVServer;
import common.Parser.*;
import IECSNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.math.BigInteger;

public class ECS {
	private confPath; 
	private Socket socket;
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
            BigInteger serverHash = MD5.encode(line[SERVER_NAME] + DELIM + line[SERVER_IP] + DELIM + line[SERVER_PORT]);
            //TODO figure out begin and end hash, does zookeeper assign it?
            ServerMetaData meta = new ServerMetaData(line[SERVER_NAME], line[SERVER_IP], Integer.parseInt(line[SERVER_PORT]), null, null);
            this.ringNetwork.put(serverHash, meta);
        }

    }

	public void addNode() {


	}

	public void removeNode() {


	}


}
