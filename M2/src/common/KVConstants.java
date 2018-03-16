package common;

import java.math.BigInteger;

public class KVConstants {
    // Message marshalling and parsing
    public static final String DELIM = "|";
    public static final String NEWLINE_DELIM = "%";
    public static final String CONFIG_DELIM = " ";
    public static final String HASH_DELIM = ":";
    public static final String ZK_SEP = "/";
    public static final String PUT_CMD = "PUT";
    public static final String GET_CMD = "GET";
    public static final String ZK_ROOT = "zoo";
    public static final int SESSION_TIMEOUT = 5000;
    public static final int LAUNCH_TIMEOUT = 10000;

    // Consistent Hashing
    public static final BigInteger MIN_HASH = new BigInteger("00000000000000000000000000000000", 16);
    public static final BigInteger MAX_HASH = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
} 
