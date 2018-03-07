package ecs;

import java.math.BigInteger;

import common.*;

public class ECSNode implements IECSNode{
    private ServerMetaData meta;
    
    public ECSNode(String name, String addr, int port, BigInteger bHash, BigInteger eHash) {
        this.meta = new ServerMetaData(name, addr, port, bHash, eHash);
    }

    public ECSNode(String dataStr) {
        this.meta = new ServerMetaData(dataStr);
    }
    
    public ECSNode(String data, String delim) {
    	this.meta = new ServerMetaData(data, delim);
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
    public BigInteger[] getNodeHashRange() {
    	BigInteger [] hashRange = {this.meta.bHash, this.meta.eHash};
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
    public void setNodeBeginHash(BigInteger bHash) {
        if(this.meta == null) {
            System.out.println("this.meta not initialized!");
        }
        this.meta.setBeginHash(bHash);
    }

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public void setNodeEndHash(BigInteger eHash) {
        this.meta.setEndHash(eHash);
    }
    
    public void printMeta() {
    	System.out.println("Server Name: " + meta.getServerName() + " ServerAddr: " + meta.getServerAddr() + " ServerPort: " + meta.getServerPort());
    	if ((meta.getBeginHash() != null) && (meta.getEndHash() != null))
    		System.out.println("Server bHash: " + meta.getBeginHash().toString(16) + " eHash: " + meta.getEndHash().toString(16));
    }
}
