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
        //M1 tests
		clientSuite.addTestSuite(InteractionTest.class); 
		clientSuite.addTestSuite(MultipleClients.class); 
		clientSuite.addTestSuite(MultipleClients.class); 
		clientSuite.addTestSuite(CacheTests.class); 
        //M2 tests
        clientSuite.addTestSuite(AdditionalTest.class); 
        //M3 tests
        clientSuite.addTestSuite(ReplicasTests.class); 
        clientSuite.addTestSuite(ThreeReplicasTests.class); 
		return clientSuite;
	}
	
}
