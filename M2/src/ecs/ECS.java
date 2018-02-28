package ecs;
import app_kvServer.KVServer;
import IECSNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;

public class ECS {

	
	private confPath; 
	private Socket socket;
    private String DELIM = "|";
    private HashMap<BigInteger, ServerMetaData> ringNetwork;
	private enum ConfigContent {
        SERVER_NAME = 0,
        SERVER_IP = 1,
        SERVER_PORT = 2;
    }
	public ECS(String confFile)
    {
        this.configFile = Paths.get(configFile);
        this.ringNetwork = new HashMap<BigInteger, ServerMetaData>();
        populateRingNetwork();
    }

    private BigInteger strToMD5(String m){
        MessageDigest hash = MessageDigest.getInstance("MD5");

        //hash.update(input, offset, len)
        hash.update(m.getBytes(), 0, m.length());

        return new BigInteger(1, hash.digest());
    }

    private void populateRingNetwork() throws IOException {
        ArrayList<String> lines = new ArrayList<>(Files.readAllLines(this.configFile, standardCharsets.UTF_8));
        int numServers = Math.floor(lines.size()/2);
        
        for(int i = 0; i < numServers ; i++) {
            String[] line = lines.get(i).split(" ");
            BigInteger serverHash = strToMD5(line[SERVER_NAME] + DELIM + line[SERVER_IP] + DELIM + line[SERVER_PORT]);
            //TODO figure out begin and end hash, does zookeeper assign it?
            ServerMetaData meta = new ServerMetaData(line[SERVER_NAME], line[SERVER_IP], Integer.parseInt(line[SERVER_PORT]), null, null)
            this.ringNetwork.put(serverHash, meta);
        }

    }

	public void addNode() {


	}

	public void removeNode() {


	}


}
