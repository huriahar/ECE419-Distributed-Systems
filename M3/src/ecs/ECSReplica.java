package ecs;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;
import java.util.HashMap;
import java.math.BigInteger;
import java.net.Socket;
import java.util.Iterator;
import java.io.IOException;
import java.lang.InterruptedException;
import java.net.SocketTimeoutException;
import java.net.SocketException;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.File;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.Collection;

import org.apache.log4j.Logger;
import java.lang.Process;

import org.apache.zookeeper.KeeperException;

import common.*;
import common.messages.TextMessage;

import java.util.concurrent.TimeUnit;

public class ECSReplica implements IECS {

    private IECSNode replica1;
    private IECSNode replica2;
    
    public String getR1Name(){
        return replica1.getNodeName();
    }

    public int getR1Port() {
        return replica1.getNodePort();
    }

    public String getR1Host() {
        return replica1.getNodeHost();
    } 

    public BigInteger getR1BeginRange() {
           return replica1.getNodeHashRange()[0];
    }

    public BigInteger getR1EndRange() {
        return replica1.getNodeHashRange()[1];
    }


    public String getR2Name(){
        return replica2.getNodeName();
    }

    public int getR2Port() {
        return replica2.getNodePort();
    }

    public String getR2Host() {
        return replica2.getNodeHost();
    } 

    public BigInteger getR2BeginRange() {
           return replica2.getNodeHashRange()[0];
    }

    public BigInteger getR2EndRange() {
        return replica2.getNodeHashRange()[1];
    }

    public void setR1Name(String name) {
        replica1.setNodeName(name);
    }
    
    public void setR1Port(int port) {
        replica1.setNodePort(port);
    }

    public void setR1Host(String host) {
        replica1.setNodeHost(host);
    }
    
    public void setR1BeginHash(BigInteger bHash) {
        replica1.setNodeBeginHash(bHash);
    }

    public void setR1EndHash(BigInteger eHash) {
        replica1.setNodeEndHash(eHash);
    }

    public void setR2Name(String name) {
        replica2.setNodeName(name);
    }
    
    public void setR2Port(int port) {
        replica2.setNodePort(port);
    }

    public void setR2Host(String host) {
        replica2.setNodeHost(host);
    }
    
    public void setR2BeginHash(BigInteger bHash) {
        replica2.setNodeBeginHash(bHash);
    }

    public void setR2EndHash(BigInteger eHash) {
        replica2.setNodeEndHash(eHash);
    }
}
