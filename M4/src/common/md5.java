package common;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class md5 {

    // Why are we using String and not BigInteger? String comparisons are highly inefficient...
    public static BigInteger encode(String m){
        MessageDigest hash = null;
        try {
            hash = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        hash.update(m.getBytes(), 0, m.length());
        BigInteger bi = new BigInteger(1, hash.digest());

        return bi;
    }
}
