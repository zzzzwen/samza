package org.apache.samza.job.dm;

import org.apache.samza.clustermanager.DMListenerEnforcer;
import org.apache.samza.clustermanager.DMListenerEnforcerRMIImpl;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;

public class DMSchedulerListenerImpl implements DMSchedulerListener, Runnable {

    DMScheduler scheduler;

    @Override
    public void startListener() {
        Thread thread = new Thread(this);
        thread.start();
    }

    @Override
    public void setScheduler(DMScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void run() {
        try {
            DMSchedulerListenerAPI listener = new DMScheduelrListenerAPIImpl(scheduler);
            Naming.rebind("rmi://127.0.0.1:2000/listener", listener);

        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }


        System.out.println("RMI server starts up");
    }
}
