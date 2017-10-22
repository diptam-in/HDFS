/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datanode;

import com.example.communication.ClientDataNode.ReadBlockRequest;
import com.example.communication.ClientDataNode.ReadBlockResponse;
import com.google.protobuf.ByteString;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author diptam
 */
public class DataReader {
    private ReadBlockRequest read;
    private static SysInfo si = new SysInfo();
    private static String key = (new DataNodeConfReader()).getDataNodeRemoteKey();
    private static String storagepath= (new DataNodeConfReader()).getStorageLocation();
    Logger logger = Logger.getLogger(this.getClass().getName()); 
    
    public DataReader(ReadBlockRequest rt)
    {
        this.read =rt;
    }
    
    public ReadBlockResponse read()
    {
        String chunkNum = (new Integer(read.getBlockNum())).toString();
        
        try {
            ByteString data = readChunk(chunkNum);
            return ReadBlockResponse.newBuilder().setData(data)
                    .setStatus(1).build();
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
            return ReadBlockResponse.newBuilder().setStatus(0).build();
        }
    }
    
    private ByteString readChunk(String chunkNum) throws IOException
    {
        String p = storagepath+chunkNum;
        FileReader fr = null;
        try {
            fr = new FileReader(p);
        } catch (FileNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        
        if(fr!=null)
        {
            fr.close();
            Path path = Paths.get(p);
            byte[] data = Files.readAllBytes(path);
            return ByteString.copyFrom(data);
        }
        return null;
    }
}
