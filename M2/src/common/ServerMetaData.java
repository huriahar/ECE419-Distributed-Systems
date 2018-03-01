package common;

public class ServerMetaData {
    public String name;
    public String addr;
    public int port;
    public String bHash;
    public String eHash;

    public static final int SERVER_NAME = 0;
    public static final int SERVER_IP = 1;
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

    public ServerMetaData(String name, String addr, int port) {
        this(name, addr, port, null, null);
    }

    public void setBeginHash(String s) {
        this.bHash = s;
    }

    public void setEndHash(String s) {
        this.eHash = s;
    }
}

