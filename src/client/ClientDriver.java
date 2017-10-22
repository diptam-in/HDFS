/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package client;

import client.interfaces.Client;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import client.interfaces.DistributedFileReadable;
import client.interfaces.DistributedFileWritable;

/**
 *
 * @author diptam
 */

/*
* Any Driver class that marks the start point of Client Side of DFS,
* must implement Client interface. This interfaqce specifies set of 
* methods those must be implemented by any Client-driver. 
*/
public class ClientDriver implements Client {
    Logger logger = Logger.getLogger(this.getClass().getName());
    public static void main(String args[])
    {
        remote.rmi.Client.security();
        /*
        * Command Line Argument:
        * $ get filename (not implemented)
        * $ put filename
        * $ list (not implemented)
        * $ mkdir directoryName (not implemented)
        */
        Client client = new ClientDriver();
        if(args.length>0)
        {
            /*
            * handle "put" command
            * check the given filename exists or not
            * IF does NOT, log error and exit.
            */
            if(args[0].equals("put"))
            {
                String filename=null;
                String path=null;
                if(args.length>1)
                    filename= args[1];
                if(args.length>2)
                    path=args[2];
                if(filename!=null)
                {
                    String default_path=(new ClientConfReader())
                            .getDefaultFileLocation();
                    try {
                        FileReader f = new FileReader(default_path+filename);  
                        f.close();
                        if(client.put(filename,path))
                           System.out.println("[File Written Successfully]");
                        else
                            System.err.println("[Can't write file]");
                    } catch (FileNotFoundException ex) {
                        Logger.getLogger(ClientDriver.class.getName())
                                .log(Level.SEVERE, "[No Such File]"
                                        +ex.getMessage());
                    } catch (IOException ex) {
                        Logger.getLogger(ClientDriver.class.getName())
                                .log(Level.SEVERE, ex.getMessage());
                    }
                }
                else
                {
                    Logger.getLogger("[Driver Method]").log(Level.WARNING,"["
                    + "File name not found]\nExiting Client Driver... \n");
                }
            }
            /*
            * handle "get" command
            * Call read method with parameter filename
            */
            else if(args[0].equals("get"))
            {
                if(args.length>1)
                {
                    String filename = args[1];
                    byte[] filedata = client.get(filename);
                    if(filedata==null)
                    {
                        Logger.getLogger("[Client]").log(Level.FINE,
                                    "[No Such File Found]\n");
                    }
                    else
                    {
                        //for(byte b: filedata)
                            System.out.print(new String(filedata));
                    }
                }
                else
                {
                    Logger.getLogger("[Driver Method]").log(Level.WARNING,"["
                    + "File name not found]\nExiting Client Driver... \n"); 
                }
            }
            /*
            * handle "get" command
            * Call list method with parameter dirname
            */
            else if(args[0].equals("ls"))
            {
                if(args.length > 1)
                {
                    String dirname = args[1];
                    System.out.println(client.list(dirname));
                }
                else
                {
                    Logger.getLogger("[Driver Method]").info("[No dir name "
                            + "specifed. Defaulting to current dir]");
                    System.out.println(client.list("."));
                }
            }
            /*
            * handle "mkdir" command
            * Call makeDirectory method with parameter dirname
            */
            else if(args[0].equals("mkdir"))
            {
                if(args.length>1)
                {
                    if(client.makeDirectory(args[1]))
                        System.out.println("[Directory Created Succesfully]");
                    else
                        System.err.println("[Cant create directory]");
                }
                else
                {
                     Logger.getLogger("[Driver Method]").log(Level.WARNING,"["
                    + "Directory name not found]\nExiting Client Driver... \n");
                }
            }
            /*
            * If given command does not fall into any recognised category
            */
            else
            {
                Logger.getLogger("[Driver Method]").log(Level.WARNING,"["
                    + "command not recognised]\nExiting Client Driver.. \n");
            }
        }
        /*
        * If no command line argument specified
        */

        else
        {
            Logger.getLogger("[Driver Method]").log(Level.WARNING,"["
                    + "command not found]\nExiting Client Driver.. \n");
        }
    }

    @Override
    public boolean put(String filename, String path) {
        DistributedFileWritable wrt = new Writer();
        return wrt.put(filename,path);
    }

    @Override
    public byte[] get(String filename) {
       DistributedFileReadable rd = new Reader();
       return rd.get(filename);
    }

    @Override
    public String list(String dirname) {
        DistributedFileReadable rl = new Reader();
        String lst=rl.list(dirname);
        if(lst==null)
            return "[No such path or Nothing in specified path]";
        return lst;
    }

    @Override
    public boolean makeDirectory(String dirname) {
        DistributedFileWritable mk = new Writer();
        return mk.makeDir(dirname);
    }
    
}
