package common;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class md5 {

    // Why are we using String and not BigInteger? String comparisons are highly inefficient...
	public static String encode(String m){
        MessageDigest hash = null;
        try {
            hash = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        //hash.update(input, offset, len)
        hash.update(m.getBytes(), 0, m.length());

        return hash.digest().toString();
    }
}
