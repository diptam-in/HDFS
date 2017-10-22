package remote.rmi;

import com.sun.istack.internal.logging.Logger;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.logging.Level;

/**
 * Created by SAYAN on 14-10-2017.
 */
public class Connection<T> {
    Registry registry;
    private T t;
    private String ip;
    private String service;
    public Connection(String ip, String service){
        this.ip = ip;
        this.service = service;
    }
    public T getStub() throws RemoteException, NotBoundException{
        T stub = null;
        Registry registry = LocateRegistry.getRegistry(this.ip);
        stub = (T) registry.lookup(this.service);
        return stub;
    }
}
