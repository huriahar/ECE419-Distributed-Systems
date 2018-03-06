package common;

public class KVConstants {
    // Message marshalling and parsing
    public static final String DELIM = "|";
    public static final String NEWLINE_DELIM = "%";
    public static final String HASH_DELIM = ":";
    public static final String PUT_CMD = "PUT";
    public static final String GET_CMD = "GET";

    // Consistent Hashing
    public static final String MIN_HASH = "0000000000000000000000000000000";
    public static final String MAX_HASH = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
} 
