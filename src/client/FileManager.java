/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * @author diptam
 */
public class FileManager {
    private String PATH;
    private String FILE_NAME=null;
    private Integer CHUNK_SIZE_IN_KB;
    private Integer CHUNK_SIZE_IN_MB;
    private RandomAccessFile raf=null;
    private FileChannel fc=null;
    private File file=null;
    private Double FILE_SIZE=null;
    
    /*
    Constructors that sets file values of file parameter
    */
    FileManager(String filename) throws FileNotFoundException
    {
        PATH= (new ClientConfReader()).getDefaultFileLocation();
        CHUNK_SIZE_IN_KB = (new ClientConfReader()).getChunkSize();
        CHUNK_SIZE_IN_MB=CHUNK_SIZE_IN_KB/1024;
        FILE_NAME=filename;
        file = new File(PATH+FILE_NAME);
        raf = new RandomAccessFile(file, "r");
        fc = raf.getChannel();
        FILE_SIZE=new Double(file.length());
    }
    
    FileManager(String filename,Integer size) throws FileNotFoundException
    {
       FILE_NAME=filename;
       CHUNK_SIZE_IN_KB=size;
       CHUNK_SIZE_IN_MB=CHUNK_SIZE_IN_KB/1024;
       file = new File(PATH+FILE_NAME);
       raf = new RandomAccessFile(file, "r");
       fc = raf.getChannel();
       FILE_SIZE=new Double(file.length());
    }
    public Integer getChunkSize()
    {
        return CHUNK_SIZE_IN_KB;
    }
    public String getFIlename()
    {
        return FILE_NAME;
    }
    
    public Double getFileSize()
    {
        return FILE_SIZE;
    }
    
    public Double getNumberOfChunk()
    {
        return FILE_SIZE/(CHUNK_SIZE_IN_KB*1024);
    }
    
    /*
    Method: to read file starting from a random positon specified as the parameter 
    Returns the chunks as byte array
    the chunk size is specified in Class varibale CHUNK_SIZE_IN_KB
    */
    public byte[] getFileChunk(Double startposition) throws IOException
    {
        Double d = new Double(CHUNK_SIZE_IN_KB*1024);
        Double size =  d > (FILE_SIZE-startposition) ? (FILE_SIZE-startposition) : d; 
        ByteBuffer storage = ByteBuffer.allocate(size.intValue());
        fc.position(startposition.longValue());
        fc.read(storage);
        return storage.array();
    }
    /*
    Checks whether any more file chunk is left or not
    */
    public boolean hasFileChunk(Double startposition)
    {
        if(startposition<FILE_SIZE)
            return true;
        return false;
    }
}
