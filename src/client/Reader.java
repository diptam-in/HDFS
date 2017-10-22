/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package client;

import client.interfaces.NameNodeProtocol;
import com.example.communication.ClientDataNode.ReadBlockRequest;
import com.example.communication.ClientDataNode.ReadBlockResponse;
import com.example.communication.ClientNameNode.OpenFileRequest;
import com.example.communication.ClientNameNode.OpenFileResponse;
import com.example.communication.ClientNameNode.CloseFileRequest;
import com.example.communication.ClientNameNode.CloseFileResponse;
import com.example.elements.BlockInfo.Block;
import com.example.elements.DataNodeInfo.DataNode;
import com.google.protobuf.ByteString;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import remote.interfaces.NameNode;
import remote.interfaces.RemoteReadable;
import remote.rmi.Connection;
import client.interfaces.DistributedFileReadable;
import com.example.communication.ClientNameNode.listRequest;
import com.example.communication.ClientNameNode.listResponse;
import com.example.elements.DirectoryInfo.Directory;
import com.example.elements.DirectoryInfo.File;

/**
 *
 * @author diptam
 */

/*
* This is a Reader class, used to read data chunks from DatNodes.
* Any Reader class of this DFS, must implement DistributedFileReadable
* and NameNodeProtocol Interfaces.
* NameNode Interface specifies the protocol methods that is used
* to setup connection and terminate connection with NameNode.
* DistributedFileReadable Interface specifies necessary methods a Reader
* class must have in order to interface with ClientDriver. A Client Driver
* will use this interface to communicate with Reader.
*/
public class Reader implements DistributedFileReadable, NameNodeProtocol{
    
    private String nameNodeIp;
    private final int mode=1;
    private String nnkey;
    private String dnkey; 
    private Logger logger = Logger.getLogger(this.getClass().getName());
    public Reader()
    {
        nameNodeIp = (new ClientConfReader()).getNameNodeIp();
        nnkey = (new ClientConfReader()).getNameNodeKey();
        dnkey = (new ClientConfReader()).getDataNodeKey();
    }
    /*
    * DistributedFileReadable Interfcace method.
    * Specifies the complete protocol to read a file from DFS.
    */
    @Override
    public byte[] get(String file) {
        ByteString buff = ByteString.copyFrom(new byte[0]);
        ByteString temp;
        /*
        * Request NameNode for blocknumbers for the file
        */
        OpenFileRequest ofreq = OpenFileRequest.newBuilder()
                .setFilename(file).setMode(mode)
                .build();
        OpenFileResponse ofresp = openFile(ofreq);
        /*
        * If, File could not be opened, return Status must be set to 0
        * IF status is 0, send a "Abort" signal to NameNode, return false
        */
        if(ofresp.getStatus()==0)
        {
            logger.warning("[Status 0 Received]");
            closeFile(CloseFileRequest.newBuilder()
                .setFilehandle(ofresp.getFilehandle())
                .setDecision(0).build());
            return null;
        }
        /*
        * For each block, call read method to read from DataNode
        */
        for(Block b : ofresp.getBlockListList())
        {
           ReadBlockRequest rreq=null;
           logger.info("[Block] "+b.toString());
           rreq = ReadBlockRequest.newBuilder().setBlockNum(b.getNum())
                   .build();
           ReadBlockResponse resp = read(rreq,b.getDataNodeList());
           if(resp.getStatus()==0)
           {
               closeFile(CloseFileRequest.newBuilder()
                .setFilehandle(ofresp.getFilehandle())
                .setDecision(0).build());
               return null;
           }
           logger.info("[Received data] "+ b.getNum()+ " [Status] "
                   +resp.getStatus());
           temp=resp.getData();
           buff=buff.concat(temp);
        }
        closeFile(CloseFileRequest.newBuilder()
                .setFilehandle(ofresp.getFilehandle())
                .setDecision(1).build());
        return buff.toByteArray();
    }
    
    @Override
    public String list(String dirName) {
        listResponse lr = list(listRequest.newBuilder().setDirName(dirName)
                                .build());
        if(lr.hasStatus() && lr.getStatus()==1)
        {
            String resp = new String();
            for(Directory d: lr.getDirectory().getDirListList())
                resp+=d.getDirName()+"(Dir)\t"+d.getTimeStamp()+"\n";
            for(File f : lr.getDirectory().getFileListList())
                resp+=f.getFileName()+"\t"+f.getTimeStamp()+"\n";
            return resp;
        }
        return null;
    }
    
    @Override
    public OpenFileResponse openFile(OpenFileRequest req) {      
        Connection<NameNode> conn = new Connection<NameNode>(nameNodeIp,nnkey);
        NameNode stub=null;
        logger.log(Level.INFO,
                "[Opening File]");
        try {
            stub = conn.getStub();
        } catch (RemoteException ex) {
            Logger.getLogger(Writer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NotBoundException ex) {
            Logger.getLogger(Writer.class.getName()).log(Level.SEVERE, null, ex);
        }
        if(stub!=null)
            try {
                return stub.openFile(req);
        } catch (RemoteException ex) {
            logger.log(Level.SEVERE, "[Remote Error] "+ex.getMessage());
        }
        return OpenFileResponse.newBuilder().setStatus(0)
                .setFilehandle(-1).build();
    }

    @Override
    public CloseFileResponse closeFile(CloseFileRequest req) {
        Connection<NameNode> conn = new Connection<NameNode>(nameNodeIp,nnkey);
        NameNode stub=null;
        logger.log(Level.INFO,
                "[Closing File]");
        try {
            stub = conn.getStub();
        } catch (RemoteException ex) {
            Logger.getLogger(Writer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NotBoundException ex) {
            Logger.getLogger(Writer.class.getName()).log(Level.SEVERE, null, ex);
        }
        if(stub!=null)
            try {
                return stub.closeFile(req);
        } catch (RemoteException ex) {
            logger.log(Level.SEVERE, "[Remote Error] "+ex.getMessage());
        }
        return CloseFileResponse.newBuilder().setStatus(false).build();
    }
    
    private ReadBlockResponse read(ReadBlockRequest rd, List<DataNode> datanodes)
    {
        for(DataNode d: datanodes)
        {
            String ip =d.getIp();
            Connection<RemoteReadable> conn = new Connection<RemoteReadable>
                                                                    (ip,dnkey);
            logger.info("[Connecting to DataNode] "+d.getIp());
            try {
                RemoteReadable remReader = conn.getStub();
                return remReader.readBlock(rd);
            } catch (RemoteException ex) {
                logger.log(Level.SEVERE, null, ex);
            } catch (NotBoundException ex) {
                logger.log(Level.SEVERE, null, ex);
            }        
        }
        
        return ReadBlockResponse.newBuilder().setStatus(0).build();
    }
    
    private listResponse list(listRequest lr) 
    {
        Connection<NameNode> conn = new Connection<NameNode>(nameNodeIp,nnkey);
        try {
            NameNode stub = conn.getStub();
            return stub.list(lr);
        } catch (RemoteException ex) {
            logger.log(Level.SEVERE, "[Can not connect to NameNode] "+ex.getMessage());
        } catch (NotBoundException ex) {
            logger.log(Level.SEVERE, "[Can not bind stub] "+ex.getMessage());
        }
        return listResponse.newBuilder().setStatus(0).build();
    }
    
}
