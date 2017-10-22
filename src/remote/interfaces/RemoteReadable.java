/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package remote.interfaces;

import com.example.communication.ClientDataNode.ReadBlockRequest;
import com.example.communication.ClientDataNode.ReadBlockResponse;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 *
 * @author diptam
 */
public interface RemoteReadable extends Remote {
    public ReadBlockResponse readBlock(ReadBlockRequest rd)
             throws RemoteException;
}
