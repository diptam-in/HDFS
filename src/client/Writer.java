/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package client;

import client.interfaces.NameNodeProtocol;
import com.example.communication.ClientDataNode.WriteBlockRequest;
import com.example.communication.ClientDataNode.WriteBlockResponse;
import com.example.communication.ClientNameNode.OpenFileRequest;
import com.example.communication.ClientNameNode.OpenFileResponse;
import com.example.communication.ClientNameNode.CloseFileRequest;
import com.example.communication.ClientNameNode.CloseFileResponse;
import com.example.elements.BlockInfo;
import com.example.elements.BlockInfo.Block;
import com.example.elements.DataNodeInfo;
import com.google.protobuf.ByteString;
import datanode.DataWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;
import remote.interfaces.NameNode;
import remote.interfaces.RemoteWritable;
import remote.rmi.Connection;
import client.interfaces.DistributedFileWritable;
import com.example.communication.ClientNameNode.CreateDirectoryRequest;
import com.example.communication.ClientNameNode.CreateDirectoryResponse;

/**
 *
 * @author diptam
 */

/*
* This is a writer class, used to write data chunks to other DatNodes.
* Any Writer class of this DFS, must implement DistributedFileWritable,
* NameNodeProtocol Interfaces.
* NameNode Interface specifies the protocol methods that is used
* to setup connection and terminate connection with NameNode.
* DistributedFileWritable Interface specifies necessary methods a Writer class
* must have in order to interface with ClientDriver. A Client Driver will
* use this interface to communicate with Writer.
*/
public class Writer implements DistributedFileWritable, NameNodeProtocol {
    
    private String nameNodeIp;
    private Integer numberOfCopies;
    private final int mode=0;
    private String nnkey;
    private String dnkey;
    private Logger logger = Logger.getLogger(this.getClass().getName());
    
    public Writer()
    {
        nameNodeIp = (new ClientConfReader()).getNameNodeIp();
        nnkey = (new ClientConfReader()).getNameNodeKey();
        dnkey = (new ClientConfReader()).getDataNodeKey();
        numberOfCopies = (new ClientConfReader()).getNumberOfCopies();
    }
    /*
    * DistributedFileWritable Interfcace method.
    * Specifies the complete protocol to write a file to DFS.
    */
    @Override
    public boolean put(String file, String path) {
        FileManager fm = null;
        try {
            fm = new FileManager(file);
        } catch (FileNotFoundException ex) {
            logger.log(Level.SEVERE,null,ex);
        }
        /*
        * If specified path name, to where the file will be created
        * is null or Does not ends with a "/", append a "/" to it.
        */
        if(path==null || !path.endsWith("/"))
            path+="/";
        /*
        * Request NameNode for blocknumbers for the file chunks.
        */
        Integer numberOfChunk = new Double(Math.ceil(fm.getNumberOfChunk()))
                                            .intValue();
        OpenFileRequest ofreq = OpenFileRequest.newBuilder()
                .setFilename(path+file).setMode(mode)
                .setNumberOfChunks(numberOfChunk).build();
        OpenFileResponse ofresp = openFile(ofreq);
        /*
        * If, File could not be opened, return Status must be set to 0
        * IF status is 0, send a "Abort" signal to NameNode, return false
        */
        if(ofresp.getStatus()==0)
        {
            closeFile(CloseFileRequest.newBuilder()
                .setFilehandle(ofresp.getFilehandle())
                .setDecision(0).build());
            return false;
        }
        /*
        * Else, If File handle is successfully opened,
        * For each block, call write method to write to DataNode.
        */
        Double currpos=0.0;
        for(Block b : ofresp.getBlockListList())
        {
            logger.info("[For Block] "+b.getNum());
            WriteBlockRequest wreq=null;
            try {
                wreq = WriteBlockRequest.newBuilder()
                        .setBlock(b).setSeqNum(numberOfCopies)
                        .setData(ByteString.copyFrom(
                            fm.getFileChunk(currpos)))
                        .build();
                                } catch (IOException ex) {
                logger.log(Level.SEVERE,
                        ex.getMessage());
            }
            currpos+=(fm.getChunkSize()*1024);
            WriteBlockResponse wresp = write(wreq);
            
            if(wresp.getCopiesWritten()<numberOfCopies/2.0)
            {
                closeFile(CloseFileRequest.newBuilder()
                .setFilehandle(ofresp.getFilehandle())
                .setDecision(0).build());
                return false;
            }
        }
        closeFile(CloseFileRequest.newBuilder()
        .setFilehandle(ofresp.getFilehandle())
        .setDecision(1).build());
        return true;
    }
    /*
    * NameNode Interface method.
    */
    @Override
    public OpenFileResponse openFile(OpenFileRequest req) {      
        Connection<NameNode> conn = new Connection<NameNode>(nameNodeIp,nnkey);
        NameNode stub=null;
        Logger.getLogger(this.getClass().getName()).log(Level.INFO,
                "[Opening File]");
        try {
            stub = conn.getStub();
        } catch (RemoteException ex) {
            logger.log(Level.SEVERE, "[Cant connect to NameNode] "+ex.getMessage());
        } catch (NotBoundException ex) {
            logger.log(Level.SEVERE, "[Can't bind stub] "+ex.getMessage());
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
    /*
    * NameNode Interface method. 
    */
    @Override
    public CloseFileResponse closeFile(CloseFileRequest req) {
        Connection<NameNode> conn = new Connection<NameNode>(nameNodeIp,nnkey);
        NameNode stub=null;
        Logger.getLogger(this.getClass().getName()).log(Level.INFO,
                "[Closing File]");
        try {
            stub = conn.getStub();
        } catch (RemoteException ex) {
            logger.log(Level.SEVERE, null, ex);
        } catch (NotBoundException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        if(stub!=null)
            try {
                return stub.closeFile(req);
        } catch (RemoteException ex) {
            logger.log(Level.SEVERE, "[Remote Error] "+ex.getMessage());
        }
        return CloseFileResponse.newBuilder().setStatus(false).build();
    }
    
    /*
    * Native method, to setup connection with DataNode, write content of 
    * "one Block" into DataNode, terminate connection after wrtiting is Done.
    * returns Success/Failure.
    */
    private WriteBlockResponse write(WriteBlockRequest req)
    {
        BlockInfo.Block b = req.getBlock();
        logger.info(req.getBlock().toString());
        /*
        * Iterate over the DataNode list to find an alive Node
        * If any Node found alive, forward the request to It.
        */
        for(DataNodeInfo.DataNode d : b.getDataNodeList())
        {
            logger.info("[For DataNode] "+d.getIp());
            Connection<RemoteWritable> con = new 
                Connection<RemoteWritable>(d.getIp(),dnkey);
            logger.info("[Connecting to DataNode] "+d.getIp());
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
                        "[Could Not Bind to Remote Machine] "
                               +ex.getMessage() + "\n Retrying...\n");
                continue;
            }
            WriteBlockResponse resp=null;
            try {
                resp = stub.writeBlock(req);
            } catch (RemoteException ex) {
                logger.log(Level.SEVERE, null, ex);
                continue;
            }
            WriteBlockResponse newresp = WriteBlockResponse.newBuilder()
                    .setCopiesWritten(resp.getCopiesWritten()).build();
            logger.log(Level.INFO,"[Returning Response]\n"
                    +newresp.toString()+"\n");

            return newresp;
            
        }
        
        /*
        * If No more copy was successfullly written
        * set numberOfCopies written to 0
        * Return response 
        */
        WriteBlockResponse resp = WriteBlockResponse.newBuilder()
                    .setCopiesWritten(0).build();
        logger.log(Level.INFO,
                "[Returning Response]\n"+resp.toString()+"\n");
        return resp;
    }

    @Override
    public boolean makeDir(String dirPath) {
        if(createDirectory(CreateDirectoryRequest.newBuilder()
        .setDirectoryname(dirPath).build()).getStatus())
            return true;
        return false;
    }
    
    private CreateDirectoryResponse createDirectory(CreateDirectoryRequest cr)
    {
        Connection<NameNode> conn = new Connection<NameNode>(nameNodeIp,nnkey);
        try {
            NameNode stub = conn.getStub();
            return stub.createDirectory(cr);
        } catch (RemoteException ex) {
            logger.log(Level.SEVERE, "[Could Not connect to Remote Machine] ", ex);
        } catch (NotBoundException ex) {
            logger.log(Level.SEVERE, "[Could Not Bind to Remote Machine] ", ex);
        }
        
        return CreateDirectoryResponse.newBuilder()
                .setStatus(false).build();
    }

    
}
