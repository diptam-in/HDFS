/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datanode;

import com.example.communication.ClientDataNode.WriteBlockRequest;
import com.example.communication.ClientDataNode.WriteBlockResponse;
import com.example.elements.BlockInfo.Block;
import com.example.elements.DataNodeInfo.DataNode;
import com.google.protobuf.ByteString;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import remote.interfaces.RemoteWritable;
import remote.rmi.Connection;

/**
 *
 * @author diptam
 */
public class DataWriter {
    
    private WriteBlockRequest wt;
    private String loc=(new DataNodeConfReader()).getStorageLocation();
    private static SysInfo si = new SysInfo();
    private static String key = (new DataNodeConfReader()).getDataNodeRemoteKey();
    Logger logger = Logger.getLogger(this.getClass().getName()); 
    
    public DataWriter(WriteBlockRequest wt)
    {
        this.wt =wt;
    }
    
    public WriteBlockResponse write()
    {
        String IP = si.getIp();
        List<DataNode> newList = new ArrayList<DataNode>();
        ByteString data = wt.getData();
        Block b = wt.getBlock();
        /*
        * Iterate of RemoteWritable list to locate RemoteWritable containing
        * own information
        */
        int index =0;
        for (DataNode d : b.getDataNodeList())
        {
            if(!d.getIp().equals(IP))
                newList.add(d);
            index++;
        }
        /*
        * Remove own information of datanode from datanode list
        */
        logger.info("[Found Own Info @ ] "+index+" [Removing..]");
        
        /*
        * Write own copy of block
        * update the datanode backup file
        */
        createAndWriteBlock(data.toByteArray(),
                new Integer(b.getNum()));
        Block temp = Block.newBuilder().setNum(b.getNum())
                .addAllDataNode(newList)
                .build();
        /*
        * Uncomment following part to store commited chunk record in backup.
        
        String backup=backup = (new DataNodeConfReader()).getDataNodeBackup();
        try {
            FileWriter fstream = new FileWriter(backup, true);
            fstream.append((new Integer(b.getNum())).toString());
            fstream.append('\n');
            fstream.close();
        } catch (IOException ex) {
            Logger.getLogger(BlockReport.class.getName()).log(Level.SEVERE,
                    "[While Opening Backup File for Writing] "+ex.getMessage());
        }
        */
        
        /*
        * Recreate new WriteBlockRequest to forward to rest of the RemoteWritable
        * This new WriteBlockRequeat will be used only once.
        */
        WriteBlockRequest newreq = WriteBlockRequest.newBuilder().setBlock(temp)
                .setSeqNum(wt.getSeqNum()-1).setData(data).build();
        
        /*
        * Iterate over the DataNode list to find an alive Node
        * If any Node found alive, forward the request to It.
        */
        for(DataNode d : newList)
        {
            Connection<RemoteWritable> con = new 
                Connection<RemoteWritable>(d.getIp(),key);
            RemoteWritable stub = null;
            try {
                stub = con.getStub();
            } catch (RemoteException ex) {
                logger.log(Level.SEVERE, 
                       "[Could Not connect to Remote Machine] "
                               +ex.getMessage() + "\n Retrying...\n");
                continue;
            } catch (NotBoundException ex) {
                logger.log(Level.SEVERE,
                        "[Could Not connect to Remote Machine] "
                               +ex.getMessage() + "\n Retrying...\n");
                continue;
            }
            WriteBlockResponse resp=null;
            try {
                resp = stub.writeBlock(newreq);
            } catch (RemoteException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
            WriteBlockResponse newresp = WriteBlockResponse.newBuilder()
                    .setCopiesWritten(resp.getCopiesWritten()+1).build();
            logger.log(Level.INFO,
                "[Returning Response]\n"+newresp.toString()+"\n");

            return newresp;
        }
        
        /*
        * If No more copy was successfullly written or This is the last DataNode
        * set numberOfCopies written to 1
        * Return response 
        */
        WriteBlockResponse resp = WriteBlockResponse.newBuilder()
                    .setCopiesWritten(1).build();
        logger.log(Level.INFO,
                "[Returning Response]\n"+resp.toString()+"\n");
        return resp;
    }
    
    private boolean createAndWriteBlock(byte[] bt, Integer blockNum)
    {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(loc+blockNum.toString());
        } catch (FileNotFoundException ex) {
            logger.log(Level.SEVERE,
                    ex.getMessage());
        }
        /*
        * If FileOutputStream succesfully opened
        * The proceed with writing into the disk.
        */
        if(fos!=null)
        {
            try {
                fos.write(bt);
            } catch (IOException ex) {
                logger.log(Level.SEVERE,
                        "[fos.write] "+ex.getMessage());
            }
            try {
                fos.close();
            } catch (IOException ex) {
                logger.log(Level.SEVERE,
                       "[fos.close] "+ex.getMessage());
            }
            logger.log(Level.INFO,
                    "Block Written to disk successfully!");
            return true;
        }
        return false;
    }

}
