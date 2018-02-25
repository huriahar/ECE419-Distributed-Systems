package ecs;

import common.ServerMetaData;

public class ECSNode {
    private ServerMetaData meta;
    public ECSNode(String name, String addr, int port, String bHash, String eHash) {
        this.meta = new ServerMetaData(name, addr, port, bHash, eHash);
    }
    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName() {
		return this.meta.name;
	}

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost() {
		return this.meta.addr;
	}
    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort() {
		return this.meta.port;
	}
    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange() {
		String [] hashRange = {this.meta.bHash, this.meta.eHash};
		return hashRange;	
	}

}
