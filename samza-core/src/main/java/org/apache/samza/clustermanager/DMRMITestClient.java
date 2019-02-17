package org.apache.samza.clustermanager;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;


/**
 * test class for running rmi client
 */

public class DMRMITestClient {

    public static void main(String[] args){

        try {
            DMListenerEnforcer enforcer = (DMListenerEnforcer) Naming.lookup("rmi://127.0.0.1:1999/listener");
            enforcer.enforceSchema(123);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }
}
