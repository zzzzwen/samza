package org.apache.samza.clustermanager;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class DMRMIListenerServer implements Runnable {
    @Override
    public void run() {
        DMListenerEnforcerRMIImpl enforcer = new DMListenerEnforcerRMIImpl();

        try {
            DMListenerEnforcer stub = (DMListenerEnforcer) UnicastRemoteObject.exportObject(enforcer, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind("RMI-Enforcer", stub);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        System.out.println("RMI server starts up");
    }
}
