/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package client;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author diptam
 */
public class ClientConfReader {
    private static Properties p=null;
    private static FileReader reader;
    
    ClientConfReader()
    {
        try {  
            reader=new FileReader("client_conf.properties");
        } catch (FileNotFoundException ex) {
            Logger.getLogger(datanode.DataNodeConfReader.class.getName()).log(Level.SEVERE,
                    null, ex);
        }
      
        p=new Properties();  
        try {  
            p.load(reader);
        } catch (IOException ex) {
            Logger.getLogger(datanode.DataNodeConfReader.class.getName()).log(Level.SEVERE,
                    null, ex);
        }
    }
    
    public String getNameNodeIp()
    {
        return p.getProperty("name_node_ip");
    }
    
    public String getDefaultFileLocation()
    {
        return p.getProperty("default_file_path");
    }
    
    public String getNameNodeKey()
    {
        return p.getProperty("namenode_key");
    }
    
    public String getDataNodeKey()
    {
        return p.getProperty("datanode_key");
    }
    
    public Integer getNumberOfCopies()
    {
        return new Integer(p.getProperty("number_of_copies"));
    }
    
    public Integer getChunkSize()
    {
        return new Integer(p.getProperty("chunk_size"));
    }
}
