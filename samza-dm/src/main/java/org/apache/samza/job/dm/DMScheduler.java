package org.apache.samza.job.dm;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.samza.config.Config;

/**
 *  Decision Maker scheduler
 */
public interface DMScheduler {
    /**
     * start the scheduler
     */
    void init(Config config);

    /**
     * submit the application to the cluster, this function is called once when job is submitted
     * @param config
     */
    void submitApplication();

    /**
     * This function calls internal allocator to calculate the resource and parallelism
     * for the given stage.
     *
     * @return Allocation resource and number of containers for this stage
     */
    Allocation allocate(Resource clusterResource);

    /**
     * returns default parallelism mapping for stages
     *
     * @return Allocation default allocation
     */
    Allocation getDefaultAllocation(String stageId);

    /**
     * create a new listerner to listen to the monitor's heartbeat
     *
     * @param scheduler the Scheduler that the listener is responding to
     */
    void createListener(DMScheduler scheduler);

    /**
     * set the dispatcher
     */
    DMDispatcher getDispatcher();

    /**
     * dispatch the schema to the enforcer
     *
     * @param allocation
     */
    void dispatch(Allocation allocation);
}
