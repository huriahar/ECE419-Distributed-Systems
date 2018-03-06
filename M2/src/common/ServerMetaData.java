package common;

public class ServerMetaData {
    public String name;
    public String addr;
    public int port;
    public String bHash;
    public String eHash;

    public static final int SERVER_NAME = 0;
    public static final int SERVER_ADDR = 1;
    public static final int SERVER_PORT = 2;
    public static final int BEGIN_HASH = 3;
    public static final int END_HASH = 4;

    public ServerMetaData(String name, String addr, int port, String bHash, String eHash) {
        this.name = name;
        this.addr = addr;
        this.port = port;
        this.bHash = bHash;
        this.eHash = eHash;
    }

    public ServerMetaData(String dataStr) {
        String[] data = dataStr.split("\\" + KVConstants.DELIM);
        this.name = data[SERVER_NAME];
        this.addr = data[SERVER_ADDR];
        this.port = Integer.parseInt(data[SERVER_PORT]);
        this.bHash = data[BEGIN_HASH];
        this.eHash = data[END_HASH];
    }

    public ServerMetaData(String name, String addr, int port) {
        this(name, addr, port, null, null);
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public void setBeginHash(String s) {
        this.bHash = s;
    }

    public void setEndHash(String s) {
        this.eHash = s;
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

}

