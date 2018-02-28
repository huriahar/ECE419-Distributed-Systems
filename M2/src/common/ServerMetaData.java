package common;

public class ServerMetaData {
    public String name;
    public String addr;
    public int port;
    public String bHash;
    public String eHash;

	public static enum configContent{
        SERVER_NAME = 0,
        SERVER_IP = 1,
        SERVER_PORT = 2,
        BEGIN_HASH = 3,
        END_HASH = 4;
    }

    public ServerMetaData(String name, String addr, int port, String bHash, String eHash) {
        this.name = name;
        this.addr = addr;
        this.port = port;
        this.bHash = bHash;
        this.eHash = eHash;
    }

    public ServerMetaData(String, name, String addr, int port) {
        this(name, addr, port, null, null);
    }
}

