/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datanode;

import com.example.communication.ClientDataNode.ReadBlockRequest;
import com.example.communication.ClientDataNode.ReadBlockResponse;
import com.example.communication.ClientDataNode.WriteBlockRequest;
import com.example.communication.ClientDataNode.WriteBlockResponse;
import java.rmi.Remote;
import java.util.logging.Level;
import java.util.logging.Logger;
import remote.interfaces.RemoteReadable;
import remote.interfaces.RemoteWritable;
import remote.rmi.Server;


/**
 *
 * @author diptam
 */
public class DataNodeDriver implements RemoteWritable,RemoteReadable {
    
    /**
     * 
     * @param args 
     * The driver function for the DataNode. All services of DataNode
     * starts  execution from this point. During Startup RemoteWritable spawns
     * two threads those send BlockReport and HeartBeat respectively.
     */
    public static void main(String args[])
    {
        Thread blockReport = new Thread(new BlockReport());
        Thread heartBeat = new Thread(new HeartBeat());
        
        /*
        Starting Block report
        */
        blockReport.start();
        Logger.getLogger("[Main Mthod] ").log(Level.INFO,
                "[Block Report] Status : spawned\n");
        /*
        Starting Heart Beat
        */
        heartBeat.start();
        Logger.getLogger("[Main Mthod] ").log(Level.INFO,
                "[Heart Beat] Status : spawned\n");
        
        /*
        Starting DataNode Listener
        */
        String key = (new DataNodeConfReader().getDataNodeRemoteKey());
        String ip = (new SysInfo().getIp());
        
        Server<RemoteWritable> server = new Server<RemoteWritable>(ip,key);
        server.listen(new DataNodeDriver());
        
    }

    /**
     *
     * @param wr
     * @return
     */
    @Override
    public WriteBlockResponse writeBlock(WriteBlockRequest wr) {
        
        WriteBlockResponse resp = null;
        if(!wr.hasSeqNum())
        {
            resp = WriteBlockResponse.newBuilder().setCopiesWritten(0).build();
            return resp;
        }
        DataWriter dt = new DataWriter(wr);
        return dt.write();
    }
    /**
     *
     * @param rd
     * @return
     */
    @Override
    public ReadBlockResponse readBlock(ReadBlockRequest rd) {
        
        if(!rd.hasBlockNum())
        {
            return ReadBlockResponse.newBuilder().setStatus(0).build();
        }
        
        DataReader dr = new DataReader(rd);
        return dr.read();
    }
}
