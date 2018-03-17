package ecs;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import common.*;

public class ECSReplica {

    private IECSNode replica1;
    private IECSNode replica2;
    private boolean replica1Alive;
    private boolean replica2Alive;
       
 
    public ECSReplica(IECSNode r1, IECSNode r2) {
        this.replica1 = r1;
        this.replica2 = r2;
        this.replica1Alive = true;
        this.replica2Alive = true;
    }

    public ECSReplica() {
        this.replica1 = null;
        this.replica2 = null;
        this.replica1Alive = false;
        this.replica2Alive = false;    	
    }

    public IECSNode getR1() {
        return this.replica1;
    }
    
    public boolean isR1Alive() {
        return this.replica1Alive;
    }
    
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

    public IECSNode getR2() {
        return this.replica2;
    }

    public boolean isR2Alive() {
        return this.replica2Alive;
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

    public void setR1(IECSNode r1) {
        this.replica1 = r1;
        this.replica1Alive = true;
    }   

    public void removeR1() {
        this.replica1 = null;
        this.replica1Alive = false;
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

    public void setR2(IECSNode r2) {
        this.replica2 = r2;
        this.replica2Alive = true;
    }   

    public void removeR2() {
        this.replica2 = null;
        this.replica2Alive = false;
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
