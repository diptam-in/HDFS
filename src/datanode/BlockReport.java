/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datanode;

import com.example.elements.BlockInfo.Block;
import com.example.report.BlockReport.BlockReportRequest;
import com.example.report.BlockReport.BlockReportRequest.Builder;
import com.example.report.BlockReport.BlockReportResponse;
import datanode.interfaces.DataRecovery;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import remote.interfaces.NameNode;
import remote.rmi.Connection;

/**
 *
 * @author diptam
 */
public class BlockReport implements Runnable{

    private static String NameNodeIp;
    private static String key;
    private static long interval;
    private static String backup;
    private static int dataNodeId;
    private static String storageLoc;
    private static BufferedReader br = null;
    private static FileReader fr = null;
    private Logger logger = Logger.getLogger(this.getClass().getName());

    public BlockReport() {
    
        NameNodeIp = (new DataNodeConfReader()).getNameNodeIp();
        key= (new DataNodeConfReader()).getNameNodeRemoteKey();
        interval = new Long((new DataNodeConfReader()).getBlockReportInterval().trim());
        backup = (new DataNodeConfReader()).getDataNodeBackup();
        storageLoc = (new DataNodeConfReader()).getStorageLocation();
        /*
        * Crreate a file to store block numbers those are already written to
        * disk. This file will be read by BlockReporter periodically and will 
        * be sent to NameNode.
        */
        try {
            FileWriter fstream = new FileWriter(backup, true);
            fstream.close();
        } catch (IOException ex) {
            logger.log(Level.SEVERE,
                    "[While Opening Backup File] "+ex.getMessage());
        }
    }
    
    /*
    * Utility method to check whether a number is numneric or not.
    * This will be used to differentiate chunk names from other files. 
    */
    private boolean isNumeric(String s)
    {  
        return s != null && s.matches("[-+]?\\d*\\.?\\d+");  
    } 
    
    /*
    * List all the files stored in the stroage locations.
    * These are actually chunk files.
    */
    private ArrayList<Integer> getListOfBlocks()
    {
        ArrayList<Integer> a = new ArrayList<Integer>(); 
        File directory = new File(storageLoc);
        File[] fList = directory.listFiles();
        if(fList==null)
        {
            logger.severe("[Malformed Default Directory Path - "
                    + "Directory Does Not Exist] "+ storageLoc);
            return a;
        }
        for (File file : fList){
            if (file.isFile()){
                if(isNumeric(file.getName()))
                    a.add((new Integer(file.getName())));
            }
        }
        return a;
    }
    
    
    
    @Override
    public void run() {
        Connection<NameNode> conn = new Connection(NameNodeIp,key);
        NameNode stub=null;
        logger.log(Level.INFO, "[NameNode"
                + " Ip] "+ NameNodeIp + " [NameNode key ] " + key
                + "[Default Storage ] " + storageLoc);
        while(true)
        {
            Builder brq = BlockReportRequest.newBuilder();    
            String currentLine;
            brq.setId(dataNodeId);
           
            for(Integer b: getListOfBlocks())
                brq.addBlockList(b);
            
            logger.warning("[Sending to NN] "+brq.getBlockListList().toString());
            try {
                stub = conn.getStub();
            } catch (RemoteException ex) {
                logger.log(Level.SEVERE,
                        ex.getMessage());
            } catch (NotBoundException ex) {
                logger.log(Level.SEVERE,
                        ex.getMessage());
            }
            
            if(stub!=null)
            {
                BlockReportResponse brsp=null;
                try {
                    brsp = stub.updateBlockInfo(brq.build());
                } catch (RemoteException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
                if(brsp!=null)
                {
                    if(brsp.getToRestore())
                    {
                        DataRecovery rm = new RecoveryManager();
                        List<Block> blockList = brsp.getBlocksList();
                        for(Block b : blockList)
                            rm.recover(b);
                    }
                    if(brsp.getToDelete())
                    {
                        DataRecovery rm = new RecoveryManager();
                        List<Integer> chunkList = brsp.getBlockListList();
                        for(Integer i : chunkList)
                            rm.remove(i.toString());
                    }
                }
            }
            
            try {
                Thread.sleep(interval);
            } catch (InterruptedException ex) {
                logger.log(Level.SEVERE,
                        null, ex);
            }
        }
       
    }
    
}
