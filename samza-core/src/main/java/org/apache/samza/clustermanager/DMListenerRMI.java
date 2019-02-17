package org.apache.samza.clustermanager;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;

public class DMListenerRMI implements DMListener, Runnable {
    @Override
    public void registerToDM() {

    }

    @Override
    public void startListener() {
//        Thread thread = new Thread(server);
//        thread.setDaemon(true);
        Thread thread = new Thread(this);
        thread.start();
    }

    @Override
    public void run() {
        try {
            DMListenerEnforcer enforcer = new DMListenerEnforcerRMIImpl();
            Naming.rebind("rmi://127.0.0.1:1999/listener", enforcer);

        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }


        System.out.println("RMI server starts up");
    }
}
