package common;

public class ServerMetaData {
    public String addr;
    public int port;
    public String bHash;
    public String eHash;

    public ServerMetaData(String addr, int port, String bHash, String eHash) {
        this.addr = addr;
        this.port = port;
        this.bHash = bHash;
        this.eHash = eHash;
    }

    public ServerMetaData(String addr, int port) {
        this(addr, port, null, null);
    }
}

