package org.apache.samza.job.dm;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class DMScheduelrListenerAPIImpl extends UnicastRemoteObject implements DMSchedulerListenerAPI {
    DMScheduler scheduler;

    protected DMScheduelrListenerAPIImpl(DMScheduler scheduler) throws RemoteException {
        this.scheduler = scheduler;
    }

    @Override
    public void updateWorkload(String data) {
        System.out.println(data);
    }
}
