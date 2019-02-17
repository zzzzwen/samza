package org.apache.samza.job.dm;

public interface DMSchedulerListener {

    /**
     * start the listener
     */
    void startListener();

    void setScheduler(DMScheduler scheduler);
}
