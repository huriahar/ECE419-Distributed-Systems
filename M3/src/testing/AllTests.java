package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.INFO);
		//	new KVServer("server7", "localhost", 50006).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		//clientSuite.addTestSuite(PerformanceTesting.class);
		clientSuite.addTestSuite(AdditionalTest.class); 
		return clientSuite;
	}
	
}