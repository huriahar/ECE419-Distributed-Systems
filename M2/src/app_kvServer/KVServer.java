package app_kvServer;

import java.net.Socket;
import java.net.ServerSocket;
import java.io.IOException;
import java.net.UnknownHostException;
import java.net.BindException;
import java.net.InetAddress;

import java.nio.file.Files;
import java.io.FileWriter;  
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import logger.LogSetup;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.io.RandomAccessFile;
import java.nio.channels.OverlappingFileLockException;

import java.util.LinkedList;
import java.util.List;

import cache.*;
import common.messages.TextMessage;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class KVServer extends Thread implements IKVServer {

	private static Logger logger = Logger.getRootLogger();
    private int serverPort;
    private ServerSocket serverSocket;
    private KVCache cache;
    private boolean running;
    private String KVServerName ;
    private static final String DELIM = "|";

    /**
	 * Start KV Server with selected name
	 * @param name			unique name of server
	 * @param zkHostname	hostname where zookeeper is running
	 * @param zkPort		port where zookeeper is running
	 */
	public KVServer(String name, String zkHostname, int zkPort) {
		// TODO Auto-generated method stub
	}

	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		return -1;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		return null;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		return IKVServer.CacheStrategy.None;
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return -1;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		return "";
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
	}

	@Override
    public void run(){
		// TODO Auto-generated method stub
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
	}

	@Override
	public void start() {
		// TODO
	}

    @Override
    public void stop() {
		// TODO
	}

    @Override
    public void lockWrite() {
		// TODO
	}

    @Override
    public void unlockWrite() {
		// TODO
	}

    @Override
    public boolean moveData(String[] hashRange, String targetName) throws Exception {
		// TODO
		return false;
	}
}
