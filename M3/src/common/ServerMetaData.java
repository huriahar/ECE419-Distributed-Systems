package common;

import java.math.BigInteger;

public class ServerMetaData {
    public String name;
    public String addr;
    public int port;
    public BigInteger bHash;
    public BigInteger eHash;

    public static final int SERVER_NAME = 0;
    public static final int SERVER_ADDR = 1;
    public static final int SERVER_PORT = 2;
    public static final int BEGIN_HASH = 3;
    public static final int END_HASH = 4;

    public ServerMetaData(String name, String addr, int port, BigInteger bHash, BigInteger eHash) {
        this.name = name;
        this.addr = addr;
        this.port = port;
        this.bHash = bHash;
        this.eHash = eHash;
    }

    public ServerMetaData(String dataStr, String delim) {
        String[] data = dataStr.split(delim);
        this.name = data[SERVER_NAME];
        this.addr = data[SERVER_ADDR];
        this.port = Integer.parseInt(data[SERVER_PORT]);
        if(data.length > 3) {
            this.bHash = new BigInteger(data[BEGIN_HASH], 16);
            this.eHash = new BigInteger(data[END_HASH], 16);
        }
        else {
            this.bHash = null;
            this.eHash = null;
        }
    }
    
    public ServerMetaData() {
        
    }

    public ServerMetaData(String dataStr) {
        this(dataStr, KVConstants.SPLIT_DELIM);
    }

    public ServerMetaData(String name, String addr, int port) {
        this(name, addr, port, null, null);
    }

    public void updateServerMetaData(String dataStr) {
        String[] data = dataStr.split(KVConstants.SPLIT_DELIM);
        this.name = data[SERVER_NAME];
        this.addr = data[SERVER_ADDR];
        this.port = Integer.parseInt(data[SERVER_PORT]);
        if(data.length > 3) {
            this.bHash = new BigInteger(data[BEGIN_HASH], 16);
            this.eHash = new BigInteger(data[END_HASH], 16);
        }
        else {
            this.bHash = null;
            this.eHash = null;
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public void setBeginHash(BigInteger s) {
        this.bHash = s;
    }

    public void setEndHash(BigInteger e) {
        this.eHash = e;
    }
    
    public String getServerName() {
        return this.name;
    }
    
    public String getServerAddr() {
        return this.addr;
    }
    
    public int getServerPort() {
        return this.port;
    }
    
    public BigInteger getBeginHash() {
        return this.bHash;
    }
    
    public BigInteger getEndHash() {
        return this.eHash;
    }
    
    public BigInteger[] getHashRange() {
        BigInteger[] hashRange = {this.bHash, this.eHash};
        return hashRange;
    }
    
    public void printMeta() {
        System.out.println("Server Name: " + getServerName() + " ServerAddr: " + getServerAddr() + " ServerPort: " + getServerPort());
        if ((getBeginHash() != null) && (getEndHash() != null))
            System.out.println("Server bHash: " + getBeginHash().toString(16) + " eHash: " + getEndHash().toString(16));
    }
}

