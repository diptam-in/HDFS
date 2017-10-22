/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package remote.interfaces;

import com.example.report.BlockReport.BlockReportRequest;
import com.example.report.BlockReport.BlockReportResponse;
import com.example.report.HeartBeat.HeartBeatResponse;
import com.example.report.HeartBeat.HeartBeatRequest;
import com.example.communication.ClientNameNode.OpenFileRequest;
import com.example.communication.ClientNameNode.OpenFileResponse;
import com.example.communication.ClientNameNode.CloseFileRequest;
import com.example.communication.ClientNameNode.CloseFileResponse;
import com.example.communication.ClientNameNode.CreateDirectoryRequest;
import com.example.communication.ClientNameNode.CreateDirectoryResponse;
import com.example.communication.ClientNameNode.listRequest;
import com.example.communication.ClientNameNode.listResponse;
import java.rmi.Remote;
import java.rmi.RemoteException;


/**
 *
 * @author diptam
 */
public interface NameNode extends Remote{
    public BlockReportResponse updateBlockInfo(BlockReportRequest br)
             throws RemoteException;
    public HeartBeatResponse sendHeartBeat(HeartBeatRequest hb)
             throws RemoteException;
    public OpenFileResponse openFile(OpenFileRequest op)
             throws RemoteException;
    public CloseFileResponse closeFile(CloseFileRequest cl)
             throws RemoteException;
    public CreateDirectoryResponse createDirectory(CreateDirectoryRequest cd)
             throws RemoteException;
    public listResponse list(listRequest lr)
             throws RemoteException;
}
