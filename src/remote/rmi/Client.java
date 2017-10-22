package remote.rmi;

/**
 * Created by SAYAN on 14-10-2017.
 */
public class Client {
    public static void security(){
        System.setProperty("java.security.policy","./security.policy");
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
    }
}
