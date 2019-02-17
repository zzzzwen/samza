package org.apache.samza.job.dm;

import java.rmi.Remote;

public interface DMSchedulerListenerAPI extends Remote {

    public void updateWorkload(String data);
}
