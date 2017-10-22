/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datanode;


import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author diptam
 */
public class SysInfo {
    private final long MEGABYTE = 1024L * 1024L;
    private Runtime runtime = Runtime.getRuntime();
    private final String interfaceName = (new DataNodeConfReader()).getInterface();
    private Logger logger = Logger.getLogger(this.getClass().getName());
    
    private long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }
    
    public int getFreeMemory()
    {
       long memory = runtime.freeMemory();
       return (int)bytesToMegabytes(memory);
    }
    
    public int getCpuLoad()
    {
        OperatingSystemMXBean operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        Double d =operatingSystemMXBean.getSystemCpuLoad();
        return d.intValue();
    }
    
    public String getIp()
    {
        NetworkInterface networkInterface=null;
        String ip=null;
        try {
            networkInterface = NetworkInterface.getByName(interfaceName);
        } catch (SocketException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        Enumeration<InetAddress> inetAddress = networkInterface.getInetAddresses();
        InetAddress currentAddress;
        currentAddress = inetAddress.nextElement();
        while(inetAddress.hasMoreElements())
        {
            currentAddress = inetAddress.nextElement();
            if(currentAddress instanceof Inet4Address && !currentAddress.isLoopbackAddress())
            {
                ip = currentAddress.toString();
                break;
            }
        }
        return ip.substring(1);
    }
}
