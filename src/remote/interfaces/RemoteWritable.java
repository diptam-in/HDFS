/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package remote.interfaces;

import com.example.communication.ClientDataNode.WriteBlockRequest;
import com.example.communication.ClientDataNode.WriteBlockResponse;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 *
 * @author diptam
 */
public interface RemoteWritable extends Remote {
    public WriteBlockResponse writeBlock(WriteBlockRequest wr)
             throws RemoteException;
}
