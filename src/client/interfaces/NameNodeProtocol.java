/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package client.interfaces;


import com.example.communication.ClientNameNode.OpenFileRequest;
import com.example.communication.ClientNameNode.OpenFileResponse;
import com.example.communication.ClientNameNode.CloseFileRequest;
import com.example.communication.ClientNameNode.CloseFileResponse;
/**
 *
 * @author diptam
 */
public interface NameNodeProtocol {
    OpenFileResponse openFile(OpenFileRequest req);
    CloseFileResponse closeFile(CloseFileRequest req);
}
