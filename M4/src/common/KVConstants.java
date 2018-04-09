package common;

import java.math.BigInteger;

public class KVConstants {
    // Message marshalling and parsing
    public static final String DELIM = "|";
    public static final String SPLIT_DELIM = "\\|";
    public static final String NEWLINE_DELIM = "%";
    public static final String CONFIG_DELIM = " ";
    public static final String HASH_DELIM = ":";
    public static final String ZK_SEP = "/";
    public static final String PUT_CMD = "PUT";
    public static final String GET_CMD = "GET";
    public static final String ZK_ROOT = "zoo";
    public static final int SESSION_TIMEOUT = 5000;
    public static final int LAUNCH_TIMEOUT = 10000;
    public static final String TIMESTAMP_DEFAULT = "00:00:00";
    public static final long SERVER_TIMESTAMP_TIMEOUT = 8000;
    public static final long TIMESTAMPER_SLEEP_TIME = 5000;
    public static final int NUM_REPLICAS = 2;
    public static final String NULL_STRING = "NULL";
    public static final String ZERO_STRING = "0";
    public static final String PREPLICA = "PREPLICA";
    public static final String SREPLICA = "SREPLICA";
    public static final String COORDINATOR = "COORDINATOR";
    public static final String SERVER = "SERVER_";
    public static final double MIN_STDEV = 1.6;
    // Consistent Hashing
    public static final BigInteger MIN_HASH = new BigInteger("00000000000000000000000000000000", 16);
    public static final BigInteger MAX_HASH = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
} 
