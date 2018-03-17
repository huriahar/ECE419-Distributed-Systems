package ecs;

import java.math.BigInteger;

public interface IECSNode {

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName();

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost();

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort();

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public BigInteger[] getNodeHashRange();

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public void setNodeName(String name);

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public void setNodeHost(String addr);
    /**
     * @return  the port number of the node (ie 8080)
     */
    public void setNodePort(int port);
    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public void setNodeBeginHash(BigInteger bash);

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public void setNodeEndHash(BigInteger eHash);
    
    public void printMeta();
}
