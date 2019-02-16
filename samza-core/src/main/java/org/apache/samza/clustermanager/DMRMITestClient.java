package org.apache.samza.clustermanager;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class DMRMITestClient {

    public static void main(String[] args){


        try {
//            String name = "RMI-Enforcer";
//            Registry registry = LocateRegistry.getRegistry("localhost");
//            DMListenerEnforcer enforcer = (DMListenerEnforcer) registry.lookup(name);
//
//            enforcer.enforceSchema(123);
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
