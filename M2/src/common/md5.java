import java.security.MessageDigest;
import java.math.BigInteger;

public class md5 {

    public static BigInteger encode(String m){
        MessageDigest hash = MessageDigest.getInstance("MD5");

        //hash.update(input, offset, len)
        hash.update(m.getBytes(), 0, m.length());

        return new BigInteger(1, hash.digest());
    }
}
