/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datanode;

import com.example.communication.ClientDataNode.ReadBlockRequest;
import com.example.communication.ClientDataNode.ReadBlockResponse;
import com.example.elements.BlockInfo.Block;
import com.example.elements.DataNodeInfo.DataNode;
import datanode.interfaces.DataRecovery;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import remote.interfaces.RemoteReadable;
import remote.rmi.Connection;

/**
 *
 * @author diptam
 */
public class RecoveryManager implements DataRecovery {

    private static String localIp;
    private static String dnkey;
    private static Logger logger;
    private static String loc;
    
    public RecoveryManager()
    {
        localIp=(new SysInfo()).getIp();
        dnkey=(new DataNodeConfReader()).getDataNodeRemoteKey();
        logger=Logger.getLogger(RecoveryManager.class.getName());
        loc=(new DataNodeConfReader()).getStorageLocation();
    }
    
    @Override
    public void recover(Block recoverBlock) {
        byte[] data = readBlockFromDataNode(recoverBlock);
        if(data!=null)
        {
            if(createAndWriteBlock(data,recoverBlock.getNum()))
                logger.info("[Recoverd successfully] "+recoverBlock.getNum());
            else
                logger.warning("[Could not recover datablock] "+recoverBlock.getNum());   
        }
        else
        {
            logger.warning("[Could not recover datablock] "+recoverBlock.getNum());
        }
    }

    @Override
    public void remove(String blockNum) {
        Path p = Paths.get(loc+blockNum);
        try {
            Files.delete(p);
            logger.info("[Deleted chunk]");
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
    
    private byte[] readBlockFromDataNode(Block b)
    {
        List<DataNode> allNode = b.getDataNodeList();
        
        for(DataNode d : allNode)
        {
            if(!d.getIp().equals(localIp))
            {
                Connection<RemoteReadable> conn = new 
                        Connection<RemoteReadable>(d.getIp(),dnkey);
                
                RemoteReadable stub=null;
                
                try {
                    stub = conn.getStub();
                } catch (RemoteException ex) {
                    logger.log(Level.SEVERE,"[Could not connect]"
                            +ex.getMessage());
                } catch (NotBoundException ex) {
                    logger.log(Level.SEVERE,"[Could not bind]"
                            +ex.getMessage());
                }
                
                if(stub!=null)
                {
                    ReadBlockRequest req =  ReadBlockRequest.newBuilder()
                            .setBlockNum(b.getNum())
                            .build();
                    ReadBlockResponse res=null;
                    try {
                        res = stub.readBlock(req);
                    } catch (RemoteException ex) {
                        logger.log(Level.SEVERE,"[Could not place read call]"
                                +ex.getMessage());
                    }
                    if(res!=null && res.getStatus()==1)
                        return res.getData().toByteArray();
                }
            }
        }
        return null;
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
