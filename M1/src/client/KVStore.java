package client;

import common.messages.KVMessage;
import java.net.UnknownHostException;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVStore implements KVCommInterface {
	private static Logger logger = Logger.getRootLogger();
	
	private String ServerAddr;
	private int ServerPort;
	private boolean running;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		this.ServerAddr = address;
		this.ServerPort = port;
		setRunning(true);
		logger.info("Connection established with address " + address + 
			" at port " + port);
	}

	@Override
	public void connect() 
			throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isRunning() {
  		return running;
  	}

  	public void setRunning(boolean run) {
		running = run;
	}
}
