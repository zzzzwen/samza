package org.apache.samza.clustermanager;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class DMRMIListenerServer implements Runnable {
    @Override
    public void run() {
//
//        try {
//            DMListenerEnforcer stub = (DMListenerEnforcer) UnicastRemoteObject.exportObject(enforcer, 0);
//            Registry registry = LocateRegistry.getRegistry();
//            registry.rebind("RMI-Enforcer", stub);
//        } catch (RemoteException e) {
//            e.printStackTrace();
//        }
        try {
            DMListenerEnforcer enforcer = new DMListenerEnforcerRMIImpl();
//            LocateRegistry.createRegistry(1999);
            Naming.rebind("rmi://127.0.0.1:1999/listener", enforcer);

        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }


        System.out.println("RMI server starts up");
    }
}
