package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Set;

public class Node {
    private String id;
    static String [] portList = {"11108","11112","11116","11120","11124"};
    static Set<String> remotePort=new HashSet<String>(Arrays.asList(portList));
    public void setSuccessor(String successor) {
        this.successor = successor;
    }

    public void setPredecessor(String predecessor) {
        this.predecessor = predecessor;
    }

    private String port;
    private String successor;
    private String predecessor;

    public Node(String port, String successor, String predecessor) {
        this.id = getNodeHash(port);
        this.port = port;
        this.successor = successor;
        this.predecessor = predecessor;
    }

    @Override
    public String toString() {
        return "port=" + port +
                ", successor=" + successor +
                ", predecessor=" + predecessor;
    }

    public String getId() {
        return id;
    }

    public String getPort() {
        return port;
    }

    public String getSuccessor() {
        return successor;
    }

    public String getPredecessor() {
        return predecessor;
    }

    public String getNodeHash(String port){
        try {
            if(remotePort.contains(port)){
                Integer temp=Integer.parseInt(port)/2;
                return genHash(temp.toString());
            }
            return genHash(port);
        } catch (NoSuchAlgorithmException e) {
            Log.e("Node Hash","Unable to get node Hash");
        }
        return null;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public String getPredecessorHash(){
        return getNodeHash(predecessor);
    }

    public String getSuccessorHash(){
        return getNodeHash(successor);
    }
}
