package org.apache.samza.clustermanager;

import java.rmi.RemoteException;

public class DMListenerEnforcerRMIImpl implements DMListenerEnforcer {

    @Override
    public void enforceSchema(int parallelism) throws RemoteException {
        System.out.println("Receiving parallelism");
        System.out.println(parallelism);
    }
}
