package ecs;

import common.*;

public class ECSNode implements IECSNode{
    private ServerMetaData meta;
    
    public ECSNode(String name, String addr, int port, String bHash, String eHash) {
        this.meta = new ServerMetaData(name, addr, port, bHash, eHash);
    }

    public ECSNode(String dataStr) {
        this.meta = new ServerMetaData(dataStr);
    }
    
    public ECSNode(String name, String addr, int port) {
    	this.meta = new ServerMetaData(name, addr, port);
    }
    
    public ECSNode() {
    	
    }

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName() {
        return this.meta.getServerName();
    }

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost() {
        return this.meta.getServerAddr();
    }
    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort() {
        return this.meta.getServerPort();
    }
    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange() {
        String [] hashRange = {this.meta.bHash, this.meta.eHash};
        return hashRange;    
    }


    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public void setNodeName(String name) {
        this.meta.name = name;
    }

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public void setNodeHost(String addr) {
        this.meta.addr = addr;
    }
    /**
     * @return  the port number of the node (ie 8080)
     */
    public void setNodePort(int port) {
        this.meta.port = port;
    }
    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public void setNodeBeginHash(String bHash) {
        this.meta.bHash = bHash;
    }

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public void setNodeEndHash(String eHash) {
        this.meta.eHash = eHash;
    }
    
    public void printMeta() {
    	System.out.println("Server Name: " + meta.getServerName() + " ServerAddr: " + meta.getServerAddr() + " ServerPort: " + meta.getServerPort());
    }
}
