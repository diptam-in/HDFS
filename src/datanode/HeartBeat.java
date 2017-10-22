/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datanode;

import com.example.report.HeartBeat.HeartBeatRequest;
import com.example.report.HeartBeat.HeartBeatResponse;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;
import remote.interfaces.NameNode;
import remote.rmi.Connection;

/**
 *
 * @author diptam
 */
public class HeartBeat implements Runnable {
     
    private static SysInfo sys = new SysInfo();
    private static int id;
    private static String NameNodeIp;
    private static String key;
    private static long interval;
    private Logger logger = Logger.getLogger(this.getClass().getName());
    
    HeartBeat()
    {
        id = new Integer((new DataNodeConfReader()).getDataNodeId());
        NameNodeIp = (new DataNodeConfReader()).getNameNodeIp();
        key = (new DataNodeConfReader()).getNameNodeRemoteKey();
        interval = new Long((new DataNodeConfReader()).getHeartBeatInterval());
        logger.log(Level.INFO,
                "\nNameNode Ip: "+NameNodeIp+
                        "\nInterval: "+ interval);
    }
    
    @Override
    public void run() {
        Connection<NameNode> con = new Connection<NameNode>(NameNodeIp,key);
        logger.log(Level.INFO, "[NameNode"
                + " Ip] "+ NameNodeIp + " [NameNode key ] " + key);
        NameNode  stub = null;
        try {
            stub = con.getStub();
        } catch (RemoteException ex) {
            logger.log(Level.SEVERE,ex.getMessage());
        } catch (NotBoundException ex) {
            logger.log(Level.SEVERE,ex.getMessage());
        }
        
        while(true)
        {
           HeartBeatRequest hr = HeartBeatRequest.newBuilder()
                   .setIsAlive(true)
                   .setId(id)
                   .setAvailableDiskMemory(sys.getFreeMemory())
                   .setAvailableMainMemory(sys.getCpuLoad())
                   .build();
           if(stub!=null)
           {
            HeartBeatResponse hb=null;
               try {
                   hb = stub.sendHeartBeat(hr);
               } catch (RemoteException ex) {
                   logger.log(Level.SEVERE, null, ex);
               }
            logger.log(Level.INFO,
                "[Received Heartbeat Response] Status: "+hb.getStatus());
           }
           else
           {
               logger.log(Level.SEVERE,
                       "[Stub Not created for heartbeat!]");
           }
            try {
                Thread.sleep(interval);
            } catch (InterruptedException ex) {
                logger.log(Level.SEVERE,
                        ex.getMessage());
            }
           
        }
    }
}
