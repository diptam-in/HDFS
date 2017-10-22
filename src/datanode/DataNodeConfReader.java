/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datanode;

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
public class DataNodeConfReader {
    private static Properties p=null;
    private static FileReader reader;
    private Logger logger = Logger.getLogger(this.getClass().getName());
    
    DataNodeConfReader()
    {
        try {  
            reader=new FileReader("conf.properties");
        } catch (FileNotFoundException ex) {
            logger.log(Level.SEVERE,null, ex);
        }
      
        p=new Properties();  
        try {  
            p.load(reader);
        } catch (IOException ex) {
            logger.log(Level.SEVERE,null, ex);
        }
    }
    
    public String getNameNodeIp()
    {
        return p.getProperty("namenode_loc");
    }
    
    public String getBlockReportInterval()
    {
        return p.getProperty("blockreport_interval");
    }
    
    public String getHeartBeatInterval()
    {
        return p.getProperty("heartbeat_interval");
    }
    
    public String getNameNodeRemoteKey()
    {
        return p.getProperty("namenode_key");
    }
    
    public String getDataNodeRemoteKey()
    {
        return p.getProperty("datanode_key");
    }
    
    public String getDataNodeBackup()
    {
        return p.getProperty("default_backup_file");
    }
    
    public String getDataNodeId()
    {
        return p.getProperty("datanode_id");
    }
    
    public String getInterface()
    {
        return p.getProperty("inet_interface");
    }
    
    public String getStorageLocation()
    {
        return p.getProperty("default_storage_location");
    }
}
